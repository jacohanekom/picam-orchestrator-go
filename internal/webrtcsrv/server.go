// Package webrtcsrv serves picam-orchestrator's WHEP-style WebRTC
// signaling plus its plain HTTP control/status endpoints, and manages
// the set of subscribed WebRTC clients.
package webrtcsrv

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"picam-orchestrator/internal/pipestat"
	"picam-orchestrator/internal/telemetry"
)

// StreamSource identifies one of the two independently configured
// resolutions this process streams.
type StreamSource int

const (
	StreamMain StreamSource = iota
	StreamLores
)

func (s StreamSource) String() string {
	if s == StreamLores {
		return "lores"
	}
	return "main"
}

// ParseStream parses a "main"/"lores" query-param value, returning def
// for anything else (including empty/absent).
func ParseStream(s string, def StreamSource) StreamSource {
	switch s {
	case "main":
		return StreamMain
	case "lores":
		return StreamLores
	default:
		return def
	}
}

// Config configures the WebRTC/control HTTP server.
type Config struct {
	HTTPPort               int
	DefaultStream          StreamSource
	ICEPortMin, ICEPortMax uint16
	PicamRawHost           string
	PicamRawCmdPort        int
	MaxClients             int
}

// Server serves WHEP signaling plus the plain control/status endpoints,
// and owns the set of currently subscribed WebRTC clients.
type Server struct {
	cfg Config
	api *webrtc.API

	// clients is a copy-on-write client list: readers (the hot broadcast
	// path, called many times a second) do a single atomic load and never
	// take a lock; writers (register/prune, rare) hold registerMu, build
	// a fresh slice, and atomically publish it. Direct translation of the
	// C++ original's atomic_load/atomic_store<shared_ptr<const vector<>>>
	// pattern onto Go's atomic.Pointer.
	clients    atomic.Pointer[[]*Client]
	registerMu sync.Mutex

	httpSrv *http.Server

	OSDCameraID    atomic.Bool
	OSDTime        atomic.Bool
	MainAnnotated  atomic.Bool
	LoresAnnotated atomic.Bool

	status    *pipestat.Status
	telemetry *telemetry.State
}

// New builds a Server. Call Start to begin listening.
func New(cfg Config, status *pipestat.Status, tel *telemetry.State) (*Server, error) {
	se := webrtc.SettingEngine{}
	if err := se.SetEphemeralUDPPortRange(cfg.ICEPortMin, cfg.ICEPortMax); err != nil {
		return nil, fmt.Errorf("webrtcsrv: invalid ICE port range %d-%d: %w", cfg.ICEPortMin, cfg.ICEPortMax, err)
	}
	// Convenience for same-host dev/testing; harmless in the real
	// deployment topology (picam-frontend is always a separate host) —
	// see plan notes for why this is a safe deviation from the C++
	// original, which never enabled it.
	se.SetIncludeLoopbackCandidate(true)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	s := &Server{cfg: cfg, api: api, status: status, telemetry: tel}
	empty := []*Client{}
	s.clients.Store(&empty)
	return s, nil
}

// Start binds the HTTP listener and begins serving in the background.
// A bind failure is fatal (matching the C++ original's hard-won fix for
// a silently-swallowed bind failure that used to rebind to a random
// port) — Start does not return on that path.
func (s *Server) Start() {
	mux := http.NewServeMux()
	s.registerHandlers(mux)

	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[HTTP] FATAL: bind() failed on port %d: %v (check what's holding the port, e.g. `sudo lsof -iTCP:%d -sTCP:LISTEN`)",
			s.cfg.HTTPPort, err, s.cfg.HTTPPort)
	}

	s.httpSrv = &http.Server{Handler: withCORS(mux)}
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[HTTP] serve error: %v", err)
		}
	}()
	log.Printf("[HTTP] Listening on :%d", s.cfg.HTTPPort)
}

// Stop shuts down the HTTP server and closes every live PeerConnection.
func (s *Server) Stop(ctx context.Context) {
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(ctx)
	}
	for _, c := range *s.clients.Load() {
		c.markDead()
	}
}

// ClientCounts returns the current live client counts, in one pass.
func (s *Server) ClientCounts() (total, main, lores int) {
	for _, c := range *s.clients.Load() {
		if !c.alive.Load() {
			continue
		}
		total++
		if c.stream == StreamMain {
			main++
		} else {
			lores++
		}
	}
	return
}

// ConsumeForceKeyframe reports whether any alive client subscribed to
// stream currently needs a forced keyframe, clearing the flag on every
// such client as it checks (read-and-clear, matching the C++ original).
func (s *Server) ConsumeForceKeyframe(stream StreamSource) bool {
	any := false
	for _, c := range *s.clients.Load() {
		if !c.alive.Load() || c.stream != stream {
			continue
		}
		if c.forceKeyframe.Swap(false) {
			any = true
		}
	}
	return any
}

// Broadcast sends an already-VP8-encoded frame to every alive client
// subscribed to stream. Non-blocking: a client whose send queue is full
// simply drops this frame rather than stalling the shared encode loop or
// any other client.
func (s *Server) Broadcast(stream StreamSource, vp8 []byte, dur time.Duration) {
	for _, c := range *s.clients.Load() {
		if !c.alive.Load() || c.stream != stream {
			continue
		}
		select {
		case c.sendCh <- sampleJob{data: vp8, dur: dur}:
		default:
		}
	}
}

// registerClient publishes a fresh, copy-on-write client list containing
// every currently-alive existing client plus newClient.
func (s *Server) registerClient(newClient *Client) {
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	old := *s.clients.Load()
	fresh := make([]*Client, 0, len(old)+1)
	for _, c := range old {
		if c.alive.Load() {
			fresh = append(fresh, c)
		}
	}
	if newClient != nil {
		fresh = append(fresh, newClient)
	}
	s.clients.Store(&fresh)
}
