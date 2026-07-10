package detect

import "sync"

// Buffer is a thread-safe, timestamp-ordered ring of recent detection
// events. Entries older than maxAge (relative to the newest pushed
// event's timestamp) are pruned on every push.
type Buffer struct {
	mu       sync.Mutex
	maxAgeUs int64
	events   []Event
}

// New creates a Buffer that retains events within maxAgeMs of the
// newest one pushed.
func New(maxAgeMs int) *Buffer {
	return &Buffer{maxAgeUs: int64(maxAgeMs) * 1000}
}

// Push appends evt and prunes anything older than maxAge relative to it.
func (b *Buffer) Push(evt Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, evt)
	newest := b.events[len(b.events)-1].TsUs
	i := 0
	for i < len(b.events) && newest-b.events[i].TsUs > b.maxAgeUs {
		i++
	}
	if i > 0 {
		b.events = b.events[i:]
	}
}

// FindNearest returns the buffered event whose timestamp is closest to
// targetTsUs, if that distance is within toleranceUs.
func (b *Buffer) FindNearest(targetTsUs, toleranceUs int64) (Event, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var best Event
	found := false
	bestDiff := int64(1<<63 - 1)
	for _, e := range b.events {
		diff := e.TsUs - targetTsUs
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			best = e
			found = true
		}
	}
	if !found || bestDiff > toleranceUs {
		return Event{}, false
	}
	return best, true
}

// Size returns the number of buffered events.
func (b *Buffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}
