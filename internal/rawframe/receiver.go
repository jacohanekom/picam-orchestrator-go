package rawframe

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// maxInFlightFrames caps how many partially-reassembled frames are kept
// at once; the oldest (lowest frameSeq) is evicted to make room for a
// new one, mirroring the C++ original's std::map<uint32_t,...>::begin()
// eviction (ordered by key, i.e. by frame sequence number).
const maxInFlightFrames = 32

// ReceiverConfig configures one UDP chunk-reassembly receiver — one
// instance is used for the "main" resolution and a separate one for
// "lores", each on its own host/port/dimensions.
type ReceiverConfig struct {
	Host          string
	Port          int
	Width         int
	Height        int
	PingEverySecs int
}

// Receiver reassembles picam-raw's chunked UDP YUV420 frames and pings
// picam-raw periodically so it keeps streaming to this socket.
type Receiver struct {
	cfg        ReceiverConfig
	frameBytes int
	onFrame    func(RawFrame)

	conn *net.UDPConn

	framesReceived atomic.Uint64
	wg             sync.WaitGroup
}

// New creates a Receiver; call Start to bind the socket and begin
// receiving.
func New(cfg ReceiverConfig, onFrame func(RawFrame)) *Receiver {
	return &Receiver{
		cfg:        cfg,
		frameBytes: cfg.Width * cfg.Height * 3 / 2, // I420: Y + U/4 + V/4
		onFrame:    onFrame,
	}
}

// Start binds the local UDP socket and begins the ping and receive
// loops, both of which run until ctx is cancelled. Start returns once
// the socket is bound (or on a bind error); it does not block for the
// lifetime of the receiver — use Wait for that.
func (r *Receiver) Start(ctx context.Context) error {
	remote, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", r.cfg.Host, r.cfg.Port))
	if err != nil {
		return fmt.Errorf("rawframe: resolve %s:%d: %w", r.cfg.Host, r.cfg.Port, err)
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return fmt.Errorf("rawframe: listen: %w", err)
	}
	if err := conn.SetReadBuffer(64 * 1024 * 1024); err != nil {
		log.Printf("[UDP] warning: SetReadBuffer failed: %v", err)
	}
	r.conn = conn

	log.Printf("[UDP] Receiving %dx%d from %s:%d", r.cfg.Width, r.cfg.Height, r.cfg.Host, r.cfg.Port)

	// Go's net.Conn.Close reliably unblocks a goroutine parked in a
	// concurrent Read/ReadFrom, so shutdown is just "close the socket
	// when ctx is done" — no receive-timeout polling loop needed (the
	// C++ original needed SO_RCVTIMEO specifically because raw close()
	// did not reliably unblock a thread blocked in recv()).
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		<-ctx.Done()
		_ = conn.Close()
	}()

	r.wg.Add(1)
	go r.pingLoop(ctx, remote)

	r.wg.Add(1)
	go r.recvLoop()

	return nil
}

// Wait blocks until all of the receiver's goroutines have exited
// (implies the context passed to Start has been cancelled).
func (r *Receiver) Wait() { r.wg.Wait() }

// StreamReady reports whether at least one frame has been received.
func (r *Receiver) StreamReady() bool { return r.framesReceived.Load() > 0 }

// WaitForStream blocks (polling) until StreamReady or timeout elapses,
// returning the final readiness state.
func (r *Receiver) WaitForStream(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.StreamReady() {
			return true
		}
		select {
		case <-ctx.Done():
			return r.StreamReady()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return r.StreamReady()
}

func (r *Receiver) pingLoop(ctx context.Context, remote *net.UDPAddr) {
	defer r.wg.Done()
	interval := time.Duration(r.cfg.PingEverySecs) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := r.conn.WriteTo([]byte("HELLO"), remote); err != nil {
			// Socket is likely closed (shutting down); just stop.
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// partialFrame accumulates chunks for one in-flight frame.
type partialFrame struct {
	meta     chunkHeader
	haveMeta bool
	chunks   map[uint16][]byte
}

func (r *Receiver) recvLoop() {
	defer r.wg.Done()
	buf := make([]byte, chunk0HeaderSize+65536)
	partial := map[uint32]*partialFrame{}

	for {
		n, _, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			return // socket closed -> shutdown
		}
		if n < headerSize {
			continue
		}

		hdr := parseHeader(buf[:n])
		if hdr.totalChunks == 0 {
			continue
		}
		hdrSize := headerSize
		if hdr.chunkSeq == 0 {
			hdrSize = chunk0HeaderSize
		}
		if n <= hdrSize {
			continue
		}

		if len(partial) > maxInFlightFrames {
			evictOldest(partial)
		}

		pf, ok := partial[hdr.frameSeq]
		if !ok {
			pf = &partialFrame{chunks: map[uint16][]byte{}}
			partial[hdr.frameSeq] = pf
		}
		if hdr.chunkSeq == 0 {
			pf.meta = hdr
			pf.haveMeta = true
		}
		payload := make([]byte, n-hdrSize)
		copy(payload, buf[hdrSize:n])
		pf.chunks[hdr.chunkSeq] = payload

		if uint16(len(pf.chunks)) != hdr.totalChunks {
			continue
		}

		frameData := make([]byte, 0, r.frameBytes)
		ok = true
		for i := uint16(0); i < hdr.totalChunks; i++ {
			c, present := pf.chunks[i]
			if !present {
				ok = false
				break
			}
			frameData = append(frameData, c...)
		}
		delete(partial, hdr.frameSeq)
		if !ok || len(frameData) < r.frameBytes {
			continue
		}
		frameData = frameData[:r.frameBytes]

		rf := RawFrame{
			Data:    frameData,
			Width:   r.cfg.Width,
			Height:  r.cfg.Height,
			Arrival: time.Now(),
		}
		if pf.haveMeta {
			rf.TimestampUs = pf.meta.timestampUs
			rf.CameraIndex = pf.meta.cameraIndex
		}
		r.onFrame(rf)
		r.framesReceived.Add(1)
	}
}

// evictOldest drops the in-flight frame with the lowest frameSeq,
// mirroring std::map<uint32_t,...>::begin() eviction order.
func evictOldest(partial map[uint32]*partialFrame) {
	if len(partial) == 0 {
		return
	}
	seqs := make([]uint32, 0, len(partial))
	for seq := range partial {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	delete(partial, seqs[0])
}
