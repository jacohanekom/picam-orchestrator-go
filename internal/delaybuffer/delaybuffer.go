// Package delaybuffer holds frames until a configured hold time has
// elapsed since arrival, then releases them in order via a non-blocking
// pull interface — used to align the annotated stream with picam-hailo's
// detection latency.
package delaybuffer

import (
	"sync"
	"time"

	"picam-orchestrator/internal/rawframe"
)

// DelayBuffer is safe for concurrent Push/Pop from different goroutines.
type DelayBuffer struct {
	mu    sync.Mutex
	q     []rawframe.RawFrame
	delay time.Duration
}

// New creates a DelayBuffer holding frames for delayMs milliseconds.
func New(delayMs int) *DelayBuffer {
	return &DelayBuffer{delay: time.Duration(delayMs) * time.Millisecond}
}

// Push appends f to the back of the queue.
func (d *DelayBuffer) Push(f rawframe.RawFrame) {
	d.mu.Lock()
	d.q = append(d.q, f)
	d.mu.Unlock()
}

// Pop drains every queued frame that has aged past the configured delay
// and returns the newest of them, without blocking. Draining all due
// frames per call — rather than just the oldest one — is load-bearing:
// the main loop calls Pop once per tick, and a tick that JPEG-encodes a
// frame can take longer than the frame arrival interval (the encode is
// synchronous in the same loop; on a Pi a main-resolution encode
// routinely exceeds the output-fps interval, which the loop even logs
// as "falling behind real time"). One-pop-per-tick under that condition
// means frames enter faster than they leave, growing the queue — at
// native main resolution that's multiple MB per frame — without bound
// for as long as anyone is streaming. Returning only the newest due
// frame is consistent with the encode path's semantics: output fps is
// below input fps, so intermediate frames are skipped by design either
// way (the non-annotated path reads a latest-frame mailbox for the same
// reason).
func (d *DelayBuffer) Pop() (rawframe.RawFrame, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for n < len(d.q) && time.Since(d.q[n].Arrival) >= d.delay {
		n++
	}
	if n == 0 {
		return rawframe.RawFrame{}, false
	}
	f := d.q[n-1]
	// Zero the vacated slots so the (large) frame buffers they point to
	// become collectable immediately — re-slicing alone keeps them
	// reachable through the shared backing array until append happens to
	// reallocate it.
	for i := 0; i < n; i++ {
		d.q[i] = rawframe.RawFrame{}
	}
	d.q = d.q[n:]
	return f, true
}

// Size returns the number of frames currently queued.
func (d *DelayBuffer) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.q)
}
