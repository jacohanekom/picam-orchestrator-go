package streamsrv

import (
	"fmt"
	"log"
	"net/http"
)

// mjpegBoundary is the multipart boundary string advertised in the
// Content-Type header and repeated before every part -- an arbitrary
// token that just needs to never appear inside a JPEG frame's own
// bytes, which "frame" trivially satisfies.
const mjpegBoundary = "frame"

// handleStream implements GET /stream?stream=main|main-low|lores,
// streaming multipart/x-mixed-replace JPEG frames to the caller
// (always picam-frontend in production) until the connection closes.
// Unlike the old WebRTC signaling handler this replaces, there's no
// negotiation step at all: the response headers go out immediately and
// frames start flowing as soon as the encode loop produces one for this
// stream, with no equivalent of "wait for a keyframe" since every JPEG
// frame is independently decodable.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if total, _, _, _ := s.ClientCounts(); total >= s.cfg.MaxClients {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "too many connections"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}

	stream := ParseStream(r.URL.Query().Get("stream"), s.cfg.DefaultStream)
	client := newClient(stream)
	s.registerClient(client)
	log.Printf("[MJPEG] client connected, stream=%s", stream)

	w.Header().Set("Content-Type", `multipart/x-mixed-replace; boundary=`+mjpegBoundary)
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case jpeg := <-client.sendCh:
			fmt.Fprintf(w, "--%s\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n", mjpegBoundary, len(jpeg))
			if _, err := w.Write(jpeg); err != nil {
				client.markDead()
				return
			}
			fmt.Fprint(w, "\r\n")
			flusher.Flush()
		case <-client.done:
			return
		case <-r.Context().Done():
			client.markDead()
			return
		}
	}
}
