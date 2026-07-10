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

// Pop returns the oldest queued frame if it has aged past the configured
// delay, without blocking. The main loop calls this every tick
// regardless of annotation mode or client count, so the buffer never
// grows unbounded and stays "warm" for when annotation is toggled on.
func (d *DelayBuffer) Pop() (rawframe.RawFrame, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.q) == 0 {
		return rawframe.RawFrame{}, false
	}
	if time.Since(d.q[0].Arrival) < d.delay {
		return rawframe.RawFrame{}, false
	}
	f := d.q[0]
	d.q = d.q[1:]
	return f, true
}

// Size returns the number of frames currently queued.
func (d *DelayBuffer) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.q)
}
