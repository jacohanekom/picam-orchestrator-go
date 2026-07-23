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

	// maxStream is the best quality this client may ever be raised to,
	// fixed at connect time from the WebRTC offer's ?stream= param.
	// Overview/thumbnail clients request "lores" and are pinned there —
	// readRTCP's adaptQuality skips them entirely, so they never adapt.
	// Detail-view clients request "main" and range between StreamLores
	// (floor) and StreamMain (ceiling) based on measured packet loss.
	maxStream StreamSource

	// stream is which broadcast this client is CURRENTLY relaying —
	// mutable, adjusted live by adaptQuality() in response to RTCP
	// packet-loss feedback (see readRTCP). Stored as int32 so it can be
	// read lock-free from the hot Broadcast/ConsumeForceKeyframe path
	// (encode-loop goroutine) while written from readRTCP's goroutine.
	stream atomic.Int32

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

func newClient(pc *webrtc.PeerConnection, track *webrtc.TrackLocalStaticSample, sender *webrtc.RTPSender, maxStream StreamSource) *Client {
	c := &Client{
		pc:        pc,
		track:     track,
		sender:    sender,
		maxStream: maxStream,
		sendCh:    make(chan sampleJob, 8),
		done:      make(chan struct{}),
	}
	c.stream.Store(int32(maxStream)) // optimistic start at the requested ceiling; adaptQuality downgrades if the connection can't sustain it
	c.alive.Store(true)
	c.forceKeyframe.Store(true) // a fresh subscriber always gets a keyframe first
	go c.writePump()
	go c.readRTCP()
	return c
}

// currentStream returns which broadcast this client is presently
// relaying (may differ from maxStream if adaptQuality has downgraded
// it).
func (c *Client) currentStream() StreamSource {
	return StreamSource(c.stream.Load())
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

// readRTCP watches RTCP feedback from the remote peer: PLI (picture
// loss indication) triggers a forced keyframe, and Receiver Reports
// drive adaptive quality (see adaptQuality). It exits on its own once
// the sender/PeerConnection is closed (ReadRTCP then returns an
// error).
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
//
// lossEMA and lastSwitch are local to this single goroutine (readRTCP is
// the only reader/writer of Receiver Report state), so they need no
// synchronization — only the resulting c.stream write needs to be
// atomic, since Broadcast/ConsumeForceKeyframe read it from the
// encode-loop goroutine.
func (c *Client) readRTCP() {
	const (
		pliMinInterval = 2 * time.Second

		// EMA smoothing: how much weight each new Receiver Report gets.
		// Receiver Reports arrive every few seconds, so this reacts
		// within a couple of reports without being knocked around by one
		// noisy sample.
		lossEMAAlpha = 0.4

		// Hysteresis: downgrade readily (8% sustained loss is already a
		// visibly struggling connection), but only upgrade back once
		// loss is nearly clean, and never within switchCooldown of the
		// last switch — without this gap, a connection hovering right at
		// the boundary would flap between resolutions every report.
		downgradeLossThreshold = 0.08
		upgradeLossThreshold   = 0.01
		switchCooldown         = 8 * time.Second
	)
	var pliSuppressed uint64
	lossEMA := -1.0 // negative sentinel: no sample yet
	var lastSwitch time.Time

	for {
		pkts, _, err := c.sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, p := range pkts {
			switch pkt := p.(type) {
			case *rtcp.PictureLossIndication:
				now := time.Now().UnixNano()
				last := c.lastKfForced.Load()
				if now-last >= int64(pliMinInterval) && c.lastKfForced.CompareAndSwap(last, now) {
					c.forceKeyframe.Store(true)
					if pliSuppressed > 0 {
						log.Printf("[WebRTC] %s client: forced keyframe on PLI (%d redundant PLIs suppressed since last)",
							c.currentStream(), pliSuppressed)
						pliSuppressed = 0
					}
				} else {
					pliSuppressed++
				}

			case *rtcp.ReceiverReport:
				if c.maxStream == StreamLores {
					continue // pinned floor client (e.g. an overview thumbnail) — no ladder to adapt
				}
				for _, rr := range pkt.Reports {
					frac := float64(rr.FractionLost) / 256.0
					if lossEMA < 0 {
						lossEMA = frac
					} else {
						lossEMA = lossEMA*(1-lossEMAAlpha) + frac*lossEMAAlpha
					}
				}
				c.adaptQuality(lossEMA, &lastSwitch, downgradeLossThreshold, upgradeLossThreshold, switchCooldown)
			}
		}
	}
}

// adaptQuality switches this client between StreamMain and StreamLores
// based on a smoothed packet-loss estimate (see readRTCP), forcing a
// keyframe on the new stream so the client's decoder gets a clean start
// rather than referencing frames from the stream it just left.
func (c *Client) adaptQuality(lossEMA float64, lastSwitch *time.Time, downThresh, upThresh float64, cooldown time.Duration) {
	now := time.Now()
	if now.Sub(*lastSwitch) < cooldown {
		return
	}
	switch current := c.currentStream(); {
	case current == StreamMain && lossEMA > downThresh:
		c.stream.Store(int32(StreamLores))
		c.forceKeyframe.Store(true)
		*lastSwitch = now
		log.Printf("[WebRTC] client downgraded main->lores (loss=%.1f%%)", lossEMA*100)
	case current == StreamLores && lossEMA < upThresh:
		c.stream.Store(int32(StreamMain))
		c.forceKeyframe.Store(true)
		*lastSwitch = now
		log.Printf("[WebRTC] client upgraded lores->main (loss=%.1f%%)", lossEMA*100)
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
