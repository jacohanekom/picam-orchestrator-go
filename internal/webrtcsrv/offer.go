package webrtcsrv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/pion/webrtc/v4"
)

var errGatherTimeout = errors.New("failed to generate answer")

type offerRequest struct {
	SDP string `json:"sdp"`
}

type answerResponse struct {
	SDP string `json:"sdp"`
}

// negotiate performs WHEP-style single-shot (non-trickle) signaling: it
// builds a send-only VP8 PeerConnection restricted to the configured
// ephemeral ICE port range with no STUN/TURN servers (LAN-only,
// always-relayed-through-picam-frontend deployment), sets the remote
// offer, creates and sets the local answer, waits for ICE gathering to
// fully complete (the single HTTP response must carry the final,
// complete SDP — there is no separate trickle-ICE signaling channel),
// and registers the resulting client for broadcast.
//
// A recover() guards this function as defense-in-depth: pion itself
// returns normal errors rather than panicking on a malformed offer, but
// a bug in this glue code must still not be able to take down the other
// (up to 49) connected clients.
//
// maxStream is the ceiling quality this client may reach — the client's
// track/broadcast subscription can be adjusted downward from it live in
// response to connection quality (see Client.adaptQuality), but never
// above it, so an overview/thumbnail request for "lores" stays pinned
// there while a "main" detail-view request can range between lores and
// main.
func (s *Server) negotiate(offerSDP string, maxStream StreamSource) (answerSDP string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in webrtc negotiation: %v", p)
		}
	}()

	pc, err := s.api.NewPeerConnection(webrtc.Configuration{
		// No ICEServers: no STUN/TURN. picam-frontend always relays, so
		// there's no direct Pi<->browser path; ICE only needs to find a
		// route between two processes on the same LAN.
	})
	if err != nil {
		return "", err
	}

	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"video", "picam-"+maxStream.String(),
	)
	if err != nil {
		pc.Close()
		return "", err
	}

	sender, err := pc.AddTrack(track)
	if err != nil {
		pc.Close()
		return "", err
	}

	client := newClient(pc, track, sender, maxStream)

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateDisconnected,
			webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateClosed:
			client.markDead()
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		pc.Close()
		return "", err
	}

	// Must be created before CreateAnswer/SetLocalDescription to avoid a
	// race with gathering completing before we start waiting on it.
	gatherComplete := webrtc.GatheringCompletePromise(pc)

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return "", err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return "", err
	}

	select {
	case <-gatherComplete:
	case <-time.After(5 * time.Second):
		pc.Close()
		return "", errGatherTimeout
	}

	final := pc.LocalDescription()
	if final == nil {
		pc.Close()
		return "", errGatherTimeout
	}

	s.registerClient(client)
	return final.SDP, nil
}

// handleOffer implements POST /webrtc/offer?stream=main|lores.
func (s *Server) handleOffer(w http.ResponseWriter, r *http.Request) {
	if total, _, _ := s.ClientCounts(); total >= s.cfg.MaxClients {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "too many connections"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 65536))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}
	var req offerRequest
	if err := json.Unmarshal(body, &req); err != nil || req.SDP == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing sdp"})
		return
	}

	stream := ParseStream(r.URL.Query().Get("stream"), s.cfg.DefaultStream)

	answerSDP, err := s.negotiate(req.SDP, stream)
	if err != nil {
		if errors.Is(err, errGatherTimeout) {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to generate answer"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return
	}
	log.Printf("[WebRTC] client connected, stream=%s", stream)
	writeJSON(w, http.StatusOK, answerResponse{SDP: answerSDP})
}
