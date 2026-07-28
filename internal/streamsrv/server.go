// Package streamsrv serves picam-orchestrator's live MJPEG streams
// (multipart/x-mixed-replace over plain HTTP, no WebRTC/ICE/SDP
// involved) plus its plain HTTP control/status endpoints, and manages
// the set of subscribed streaming clients.
package streamsrv

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"picam-orchestrator/internal/irlight"
	"picam-orchestrator/internal/luxswitch"
	"picam-orchestrator/internal/pipestat"
	"picam-orchestrator/internal/recorder"
	"picam-orchestrator/internal/telemetry"
	"picam-orchestrator/internal/uistate"
)

// StreamSource identifies one of the streams this process broadcasts:
// two independently-JPEG-quality encodes of the native-resolution main
// feed (StreamMainHigh/StreamMainLow — picam-frontend picks between
// them per browser viewer based on that viewer's own downstream
// connection quality; see internal/relay's quality-switching logic in
// picam-frontend-go), plus the always-available lores feed used for
// grid-view thumbnails (unrelated to connection quality — always
// requested unconditionally). Every stream here is flat/pinned: a
// client requests exactly one and keeps it for the life of the
// connection. There is deliberately no server-side adaptation at this
// layer — the frontend↔orchestrator link is LAN-only and effectively
// always clean, so the connection whose quality is actually worth
// reacting to is the browser↔frontend leg, one hop further out, which
// is where the real adaptation lives.
type StreamSource int

const (
	StreamMainHigh StreamSource = iota
	StreamMainLow
	StreamLores
)

func (s StreamSource) String() string {
	switch s {
	case StreamMainLow:
		return "main-low"
	case StreamLores:
		return "lores"
	default:
		return "main"
	}
}

// ParseStream parses a "main"/"main-low"/"lores" query-param value,
// returning def for anything else (including empty/absent). "main" is
// kept as a friendly alias for "main-high", the ceiling tier.
func ParseStream(s string, def StreamSource) StreamSource {
	switch s {
	case "main", "main-high":
		return StreamMainHigh
	case "main-low":
		return StreamMainLow
	case "lores":
		return StreamLores
	default:
		return def
	}
}

// Config configures the streaming/control HTTP server.
type Config struct {
	HTTPPort        int
	DefaultStream   StreamSource
	PicamRawHost    string
	PicamRawCmdPort int
	MaxClients      int

	// IRLightRelayHost/Port locate pi-relay-control for the manual
	// GET /ir-light/trigger endpoint -- the automatic dark/sunrise
	// triggers already get these directly as irlight.Run params, but
	// the manual-trigger HTTP handler needs its own copy to call
	// irlight.Trigger with.
	IRLightRelayHost string
	IRLightRelayPort int

	// RecorderDir is picam-recorder's own output directory, read
	// directly for GET /events and /events/download -- see
	// internal/recorder.ListRecordings.
	RecorderDir string

	// DebugFrameJPEG, if set, JPEG-encodes the current frame for the
	// given stream straight from its live mailbox -- for the
	// GET /debug/frame.jpg diagnostic. Unlike GET /stream this is a
	// single still image with no client registration, useful for a
	// quick `curl` check of whether the frame feeding the encoder is
	// already corrupt. nil disables the endpoint.
	DebugFrameJPEG func(stream StreamSource) ([]byte, bool)

	// DebugFrameRaw, if set, returns the current raw I420 frame bytes for
	// the given stream plus its width/height, straight from the mailbox
	// with no re-encode at all — for GET /debug/frame.raw. Lets exact
	// bytes be pulled off a headless box for offline analysis.
	DebugFrameRaw func(stream StreamSource) (data []byte, w, h int, ok bool)
}

// Server serves live MJPEG streaming plus the plain control/status
// endpoints, and owns the set of currently subscribed streaming
// clients.
type Server struct {
	cfg Config

	// clients is a copy-on-write client list: readers (the hot broadcast
	// path, called many times a second) do a single atomic load and never
	// take a lock; writers (register/prune, rare) hold registerMu, build
	// a fresh slice, and atomically publish it.
	clients    atomic.Pointer[[]*Client]
	registerMu sync.Mutex

	httpSrv *http.Server

	OSDCameraID    atomic.Bool
	OSDTime        atomic.Bool
	MainAnnotated  atomic.Bool
	LoresAnnotated atomic.Bool

	status      *pipestat.Status
	telemetry   *telemetry.State
	luxSwitch   *luxswitch.State
	uiState     *uistate.State
	irLight     *irlight.State
	evtRecorder *recorder.EventRecorder
}

// New builds a Server. Call Start to begin listening.
func New(cfg Config, status *pipestat.Status, tel *telemetry.State, lux *luxswitch.State, ui *uistate.State, ir *irlight.State, rec *recorder.EventRecorder) (*Server, error) {
	s := &Server{cfg: cfg, status: status, telemetry: tel, luxSwitch: lux, uiState: ui, irLight: ir, evtRecorder: rec}
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

// Stop shuts down the HTTP server and marks every live streaming
// client dead — this unblocks each GET /stream handler's own goroutine
// (still blocked in its write loop) so they return and let the server
// finish shutting down.
func (s *Server) Stop(ctx context.Context) {
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(ctx)
	}
	for _, c := range *s.clients.Load() {
		c.markDead()
	}
}

// ClientCounts returns the current live client counts, in one pass.
func (s *Server) ClientCounts() (total, mainHigh, mainLow, lores int) {
	for _, c := range *s.clients.Load() {
		if !c.alive.Load() {
			continue
		}
		total++
		switch c.stream {
		case StreamMainHigh:
			mainHigh++
		case StreamMainLow:
			mainLow++
		default:
			lores++
		}
	}
	return
}

// Broadcast sends an already-JPEG-encoded frame to every alive client
// currently relaying stream. Non-blocking: a client whose send queue is
// full simply drops this frame rather than stalling the shared encode
// loop or any other client. Unlike VP8, JPEG has no inter-frame
// dependency, so a dropped frame here is just a skipped frame on that
// one client's stream -- no prediction chain to break, no keyframe to
// wait for.
func (s *Server) Broadcast(stream StreamSource, jpeg []byte) {
	for _, c := range *s.clients.Load() {
		if !c.alive.Load() || c.stream != stream {
			continue
		}
		select {
		case c.sendCh <- jpeg:
		default:
			total := c.droppedFrames.Add(1)
			now := time.Now().UnixNano()
			last := c.lastDropLogged.Load()
			if now-last > int64(time.Second) && c.lastDropLogged.CompareAndSwap(last, now) {
				log.Printf("[MJPEG] %s client send queue full — dropped frame (total dropped: %d)", stream, total)
			}
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
