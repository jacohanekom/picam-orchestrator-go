// Package pipestat holds the shared pipeline counters read by both the
// plain-text status protocol and /status.json.
package pipestat

import "sync"

// Status is the process-wide pipeline counter set, safe for concurrent
// access. The zero value is ready to use.
type Status struct {
	mu               sync.Mutex
	framesIn         uint64
	framesOut        uint64
	matched          uint64
	delayBufferDepth int
	clients          int
	latestFrameTsUs  int64
}

// Snapshot is a plain-value copy of Status for callers that shouldn't
// hold the mutex.
type Snapshot struct {
	FramesIn         uint64
	FramesOut        uint64
	Matched          uint64
	DelayBufferDepth int
	Clients          int
	LatestFrameTsUs  int64
}

// Snapshot returns a copy of the current counters.
func (s *Status) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{
		FramesIn:         s.framesIn,
		FramesOut:        s.framesOut,
		Matched:          s.matched,
		DelayBufferDepth: s.delayBufferDepth,
		Clients:          s.clients,
		LatestFrameTsUs:  s.latestFrameTsUs,
	}
}

// AddFramesIn increments frames_in by one — called once per reassembled
// MAIN-resolution UDP frame only (lores frames don't count), matching
// the C++ original.
func (s *Status) AddFramesIn() {
	s.mu.Lock()
	s.framesIn++
	s.mu.Unlock()
}

// SetTick updates the counters derived once per main-loop tick.
//
// Preserved quirks from the C++ original (flagged, not silently fixed,
// in case something downstream depends on them): frames_out increments
// by at most 1 per tick even if both resolutions encoded a frame that
// tick (mirrored here by newestTsUs being a single scalar the caller
// overwrites per-resolution, exactly as upstream does); if both streams
// encode in the same tick, whichever resolution's timestamp the caller
// evaluated last wins as "newest".
func (s *Status) SetTick(delayBufferDepth, clients int, matchedThisTick uint64, newestTsUs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delayBufferDepth = delayBufferDepth
	s.clients = clients
	s.matched += matchedThisTick
	if newestTsUs != 0 {
		s.framesOut++
		s.latestFrameTsUs = newestTsUs
	}
}
