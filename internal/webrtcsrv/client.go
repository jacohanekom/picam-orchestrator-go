package webrtcsrv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// sampleJob is one encoded frame queued for delivery to a client.
type sampleJob struct {
	data []byte
	dur  time.Duration
}

// Client is one subscribed WebRTC peer (always exactly picam-frontend in
// production, potentially many simultaneous instances up to the
// server's client cap).
type Client struct {
	pc     *webrtc.PeerConnection
	track  *webrtc.TrackLocalStaticSample
	sender *webrtc.RTPSender
	stream StreamSource

	alive         atomic.Bool
	forceKeyframe atomic.Bool

	// Diagnostics for a dropped-frame theory: if the encode loop outruns
	// this client's consumption (e.g. main's ~13x larger frames vs.
	// lores under real-time CPU pressure), Broadcast silently drops
	// samples rather than blocking. VP8 is predictive, so a dropped
	// delta frame breaks every subsequent frame's reference chain until
	// the next keyframe — which would look exactly like sustained
	// on-screen corruption rather than a one-frame glitch.
	droppedFrames  atomic.Uint64
	lastDropLogged atomic.Int64 // UnixNano; 0 = never logged

	// sendCh + writePump decouple Broadcast (the hot, single-goroutine
	// encode-loop path) from pion's WriteSample, which performs a
	// synchronous OS write once the connection is up (unlike
	// libdatachannel, pion does not dispatch to its own internal I/O
	// goroutines) — without this, one slow/stuck client could stall
	// delivery to the encoder or every other client.
	sendCh   chan sampleJob
	done     chan struct{}
	doneOnce sync.Once
}

func newClient(pc *webrtc.PeerConnection, track *webrtc.TrackLocalStaticSample, sender *webrtc.RTPSender, stream StreamSource) *Client {
	c := &Client{
		pc:     pc,
		track:  track,
		sender: sender,
		stream: stream,
		sendCh: make(chan sampleJob, 4),
		done:   make(chan struct{}),
	}
	c.alive.Store(true)
	c.forceKeyframe.Store(true) // a fresh subscriber always gets a keyframe first
	go c.writePump()
	go c.readRTCP()
	return c
}

func (c *Client) writePump() {
	for {
		select {
		case job := <-c.sendCh:
			_ = c.track.WriteSample(media.Sample{Data: job.data, Duration: job.dur})
		case <-c.done:
			return
		}
	}
}

// readRTCP watches for PLI (picture loss indication) feedback from the
// remote peer and requests a forced keyframe in response. It exits on
// its own once the sender/PeerConnection is closed (ReadRTCP then
// returns an error).
func (c *Client) readRTCP() {
	for {
		pkts, _, err := c.sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, p := range pkts {
			if _, ok := p.(*rtcp.PictureLossIndication); ok {
				c.forceKeyframe.Store(true)
			}
		}
	}
}

// markDead flags the client dead (excluded from future broadcasts/counts
// on its next list rebuild), stops its write pump, and asynchronously
// closes its PeerConnection. Safe to call more than once or
// concurrently.
func (c *Client) markDead() {
	if !c.alive.CompareAndSwap(true, false) {
		return
	}
	c.doneOnce.Do(func() { close(c.done) })
	go c.pc.Close()
}
