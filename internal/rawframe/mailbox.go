package rawframe

import "sync"

// Mailbox holds the single most recently received frame for a
// resolution's zero-delay "live" streaming path — continuously
// overwritten by the UDP receiver's callback, independent of whether
// annotation mode or any clients are currently active.
type Mailbox struct {
	mu    sync.Mutex
	frame RawFrame
	has   bool
}

// Set overwrites the mailbox with f.
func (m *Mailbox) Set(f RawFrame) {
	m.mu.Lock()
	m.frame = f
	m.has = true
	m.mu.Unlock()
}

// Get returns a copy of the current frame, or ok=false if nothing has
// been set yet.
func (m *Mailbox) Get() (RawFrame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.frame, m.has
}
