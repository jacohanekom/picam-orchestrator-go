package main

import "sync/atomic"

// jpegMailbox holds the latest already-JPEG-encoded frame relayed from
// picam-recorder's own GET /stream (see internal/recorderstream) --
// "latest wins", the same single-slot semantics rawframe.Mailbox already
// uses for raw frames, just for bytes that arrive pre-encoded instead of
// needing this process's own encode step.
type jpegMailbox struct {
	p atomic.Pointer[[]byte]
}

func (m *jpegMailbox) Set(jpg []byte) {
	m.p.Store(&jpg)
}

func (m *jpegMailbox) Get() ([]byte, bool) {
	p := m.p.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}
