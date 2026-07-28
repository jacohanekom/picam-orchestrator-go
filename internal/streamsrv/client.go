package streamsrv

import (
	"sync"
	"sync/atomic"
)

// Client is one subscribed MJPEG stream client (always exactly
// picam-frontend in production, potentially many simultaneous
// instances up to the server's client cap). It's a pure data/
// coordination structure -- the actual HTTP response writing happens
// in the GET /stream handler goroutine that owns this Client (see
// stream.go's handleStream), which reads sendCh directly. Unlike the
// old WebRTC design, there's no separate connection object owned here
// at all: the handler's own goroutine *is* the delivery mechanism, and
// returning from it is what closes the underlying HTTP connection.
type Client struct {
	// stream is which broadcast this client relays, fixed for the whole
	// connection's lifetime — picked by the client itself (via the
	// request's own ?stream= param) and never adapted server-side. Real
	// connection-quality adaptation happens one hop further out, on the
	// browser↔picam-frontend leg (picam-frontend's own relay), since the
	// leg this server sees is LAN-only and effectively always clean —
	// see StreamSource's doc comment.
	stream StreamSource

	alive atomic.Bool

	// Diagnostics for a dropped-frame theory: if the encode loop outruns
	// this client's consumption (e.g. main's much larger frames under
	// real-time CPU pressure), Broadcast silently drops samples rather
	// than blocking. Unlike VP8, JPEG has no inter-frame dependency, so
	// a dropped frame here is just one skipped frame on this client's
	// stream, not a corrupted prediction chain.
	droppedFrames  atomic.Uint64
	lastDropLogged atomic.Int64 // UnixNano; 0 = never logged

	// sendCh decouples Broadcast (the hot, single-goroutine encode-loop
	// path) from this client's own HTTP response write, which can block
	// on a slow/stuck downstream connection — without this, one such
	// client could stall delivery to every other client. Sized to 8 to
	// absorb a transient encode-time spike on the CPU-heavy main stream
	// without dropping — still small enough that a genuinely stuck
	// client fills it and starts dropping within one second at typical
	// live FPS, so this doesn't mask a truly wedged client.
	sendCh   chan []byte
	done     chan struct{}
	doneOnce sync.Once
}

func newClient(stream StreamSource) *Client {
	c := &Client{
		stream: stream,
		sendCh: make(chan []byte, 8),
		done:   make(chan struct{}),
	}
	c.alive.Store(true)
	return c
}

// markDead flags the client dead (excluded from future broadcasts/counts
// on its next list rebuild) and unblocks its GET /stream handler
// goroutine, which is what actually closes the underlying HTTP
// connection by returning. Safe to call more than once or concurrently.
func (c *Client) markDead() {
	if !c.alive.CompareAndSwap(true, false) {
		return
	}
	c.doneOnce.Do(func() { close(c.done) })
}
