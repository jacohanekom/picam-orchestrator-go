package webrtcsrv

import (
	"log"
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
	lastKfForced   atomic.Int64 // UnixNano of last PLI-honored keyframe; rate-limits PLI storms

	// sendCh + writePump decouple Broadcast (the hot, single-goroutine
	// encode-loop path) from pion's WriteSample, which performs a
	// synchronous OS write once the connection is up (unlike
	// libdatachannel, pion does not dispatch to its own internal I/O
	// goroutines) — without this, one slow/stuck client could stall
	// delivery to the encoder or every other client. Sized to 8 (was 4)
	// to absorb a transient encode-time spike on the CPU-heavy main
	// stream without dropping — still small enough that a genuinely
	// stuck client fills it and starts dropping within one second at
	// typical live FPS, so this doesn't mask a truly wedged client.
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
		sendCh: make(chan sampleJob, 8),
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
//
// PLIs are rate-limited to at most one honored keyframe per pliMinInterval.
// Without this, a large main-stream keyframe that loses even one RTP
// packet in transit makes the browser send a PLI, which forces another
// (equally large, equally loss-prone) keyframe, which the encoder is now
// spending all its time producing back-to-back — starving delta frames,
// deepening the send-queue backlog, and losing packets again. That is a
// stable non-recovering equilibrium the browser can't escape, and it
// only bites the main stream because only its keyframes are big enough to
// routinely lose a packet. Ignoring redundant PLIs lets the encoder keep
// producing normal frames between keyframe attempts.
func (c *Client) readRTCP() {
	const pliMinInterval = 2 * time.Second
	var pliSuppressed uint64
	for {
		pkts, _, err := c.sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, p := range pkts {
			if _, ok := p.(*rtcp.PictureLossIndication); ok {
				now := time.Now().UnixNano()
				last := c.lastKfForced.Load()
				if now-last >= int64(pliMinInterval) && c.lastKfForced.CompareAndSwap(last, now) {
					c.forceKeyframe.Store(true)
					if pliSuppressed > 0 {
						log.Printf("[WebRTC] %s client: forced keyframe on PLI (%d redundant PLIs suppressed since last)",
							c.stream, pliSuppressed)
						pliSuppressed = 0
					}
				} else {
					pliSuppressed++
				}
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
