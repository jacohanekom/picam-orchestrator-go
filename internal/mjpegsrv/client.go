package mjpegsrv

import (
	"sync"
	"sync/atomic"
)

// Client is one subscribed HTTP viewer (a browser directly, or more
// typically picam-frontend relaying to one). stream is fixed for the
// client's whole lifetime — unlike the previous WebRTC implementation,
// there's no per-client adaptive quality here: MJPEG-over-HTTP has no
// RTCP-equivalent feedback channel to adapt from, so overview/detail
// resolution choice is made once, by whichever URL the client requested.
type Client struct {
	stream StreamSource

	alive atomic.Bool

	// Diagnostics: if the encode loop outruns this client's consumption,
	// Broadcast silently drops samples rather than blocking. Unlike the
	// old VP8 path, a dropped JPEG frame has no effect on any other
	// frame (no inter-frame prediction) — purely a cosmetic one-frame
	// skip, not a corruption risk.
	droppedFrames  atomic.Uint64
	lastDropLogged atomic.Int64 // UnixNano; 0 = never logged

	// sendCh decouples Broadcast (the hot, single-goroutine encode-loop
	// path) from the per-client HTTP handler's write loop, which
	// performs a synchronous, potentially slow network write — without
	// this, one slow/stuck client could stall delivery to every other
	// client. Small buffer: a queued-but-stale JPEG frame is never worth
	// showing once a fresher one exists, so this deliberately favors
	// dropping over buffering.
	sendCh   chan []byte
	done     chan struct{}
	doneOnce sync.Once
}

func newClient(stream StreamSource) *Client {
	c := &Client{
		stream: stream,
		sendCh: make(chan []byte, 2),
		done:   make(chan struct{}),
	}
	c.alive.Store(true)
	return c
}

// markDead flags the client dead (excluded from future broadcasts/counts
// on its next list rebuild) and signals its HTTP handler goroutine to
// return. Safe to call more than once or concurrently.
func (c *Client) markDead() {
	if !c.alive.CompareAndSwap(true, false) {
		return
	}
	c.doneOnce.Do(func() { close(c.done) })
}
