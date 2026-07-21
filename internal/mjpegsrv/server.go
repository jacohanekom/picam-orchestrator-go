// Package mjpegsrv serves picam-orchestrator's live main/lores streams
// as HTTP multipart/x-mixed-replace MJPEG (the classic <img src="...">
// -compatible IP-camera format) plus the plain HTTP control/status
// endpoints, and manages the set of subscribed viewers.
//
// This replaces an earlier WebRTC/VP8 implementation: VP8 has no
// hardware encode path on any Raspberry Pi, and software VP8 encoding
// (motion estimation, inter-frame prediction) was overloading the CPU.
// JPEG has neither — every frame is encoded independently — so it's
// inherently far cheaper even in pure software (reusing the same
// stdlib encoder internal/snapshot already used for event snapshots),
// and it drops WebRTC's ICE/DTLS/SFU machinery entirely in favor of a
// plain HTTP connection per viewer. The trade-off: no built-in
// adaptive bitrate/resolution switching (there's no RTCP feedback
// channel to adapt from) and no sub-second WebRTC-grade latency — both
// judged not worth the CPU and complexity cost on this hardware.
package mjpegsrv

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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

// Config configures the MJPEG/control HTTP server.
type Config struct {
	HTTPPort        int
	DefaultStream   StreamSource
	PicamRawHost    string
	PicamRawCmdPort int
	MaxClients      int

	// DebugFrameJPEG, if set, JPEG-encodes the current frame for the
	// given stream straight from its live mailbox — bypassing the
	// normal encode/broadcast path entirely — for the GET
	// /debug/frame.jpg diagnostic. nil disables the endpoint.
	DebugFrameJPEG func(stream StreamSource) ([]byte, bool)

	// DebugFrameRaw, if set, returns the current raw I420 frame bytes for
	// the given stream plus its width/height, straight from the mailbox
	// with no re-encode at all — for GET /debug/frame.raw. Lets exact
	// bytes be pulled off a headless box for offline analysis.
	DebugFrameRaw func(stream StreamSource) (data []byte, w, h int, ok bool)
}

// Server serves MJPEG streaming plus the plain control/status
// endpoints, and owns the set of currently subscribed viewers.
type Server struct {
	cfg Config

	// clients is a copy-on-write client list: readers (the hot broadcast
	// path, called once per encoded frame) do a single atomic load and
	// never take a lock; writers (register/prune, rare — only on
	// connect/disconnect) hold registerMu, build a fresh slice, and
	// atomically publish it.
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
func New(cfg Config, status *pipestat.Status, tel *telemetry.State) *Server {
	s := &Server{cfg: cfg, status: status, telemetry: tel}
	empty := []*Client{}
	s.clients.Store(&empty)
	return s
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
		// keepAliveListener, not the raw ln: calling Serve directly on a
		// plain net.Listen listener (rather than going through
		// http.Server.ListenAndServe, which wraps it this same way
		// internally) skips TCP keepalive entirely. That matters a lot
		// here specifically: these are long-lived MJPEG streaming
		// responses to viewers on WiFi/mobile, which routinely vanish
		// without ever sending a FIN (screen lock, backgrounded app,
		// walked out of range, a NAT table entry timing out). Without
		// keepalive, the only way the server ever notices is a write
		// failing — which needs the OS's own TCP retransmission timeout
		// on unacked data to finally give up, which can take tens of
		// minutes (or effectively never, in a blackhole-routing case).
		// Until then the handler goroutine, its buffered frames, and the
		// OS socket buffers all stay alive, and the zombie still counts
		// toward MaxClients — a slow, silent leak on every viewer that
		// ever disconnects uncleanly, which is the common case for this
		// kind of client.
		if err := s.httpSrv.Serve(keepAliveListener{ln.(*net.TCPListener)}); err != nil && err != http.ErrServerClosed {
			log.Printf("[HTTP] serve error: %v", err)
		}
	}()
	log.Printf("[HTTP] Listening on :%d", s.cfg.HTTPPort)
}

// keepAliveListener enables TCP keepalive on every accepted connection —
// the same wrapper net/http's own ListenAndServe applies internally
// (there called tcpKeepAliveListener), needed here because Start calls
// net.Listen + Serve directly instead of going through ListenAndServe.
// A 30s period rather than the stdlib default of 3 minutes: this
// server's connections are almost all long-lived MJPEG streams to
// WiFi/mobile clients that routinely vanish without a clean close, so
// detecting that promptly matters more here than it does for typical
// short-lived HTTP request/response traffic.
type keepAliveListener struct {
	*net.TCPListener
}

func (ln keepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	_ = c.SetKeepAlive(true)
	_ = c.SetKeepAlivePeriod(30 * time.Second)
	return c, nil
}

// Stop shuts down the HTTP server and disconnects every live client.
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

// Broadcast sends an already-JPEG-encoded frame to every alive client
// subscribed to stream. Non-blocking: a client whose send queue is full
// simply drops this frame rather than stalling the shared encode loop
// or any other client — unlike VP8, dropping a JPEG frame has no effect
// on any other frame (no inter-frame prediction), so this is a purely
// cosmetic one-frame skip for that client, not a corruption risk.
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
