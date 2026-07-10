// Package telemetry receives picam-raw's newline-delimited JSON
// telemetry stream (lux + active camera) over an auto-reconnecting TCP
// connection and exposes the latest reading.
package telemetry

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

// State is the latest telemetry reading, safe for concurrent access.
type State struct {
	mu           sync.Mutex
	lux          float32
	activeCamera int
	cameraLabel  string
	connected    bool
}

// Snapshot is a plain-value copy of State for callers that shouldn't
// hold the mutex (e.g. building an HTTP JSON response).
type Snapshot struct {
	Connected    bool
	Lux          float32
	ActiveCamera int
	CameraLabel  string
}

func (s *State) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{
		Connected:    s.connected,
		Lux:          s.lux,
		ActiveCamera: s.activeCamera,
		CameraLabel:  s.cameraLabel,
	}
}

func (s *State) setConnected(c bool) {
	s.mu.Lock()
	s.connected = c
	s.mu.Unlock()
}

type wireState struct {
	Lux          float32 `json:"lux"`
	ActiveCamera int     `json:"active_camera"`
	CameraLabel  string  `json:"camera_label"`
}

func (s *State) apply(w wireState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lux = w.Lux
	s.activeCamera = w.ActiveCamera
	if w.CameraLabel != "" {
		// Sticky: only overwrite on a non-empty label, same as the C++
		// original, so a momentarily-blank label doesn't clobber the
		// last-known-good one.
		s.cameraLabel = w.CameraLabel
	}
}

// Run connects to picam-raw's telemetry stream at host:port,
// auto-reconnecting with a 2-second backoff, until ctx is cancelled.
func Run(ctx context.Context, host string, port int, state *State) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			if !sleepOrDone(ctx, 2*time.Second) {
				return
			}
			continue
		}
		log.Printf("[Telemetry] Connected to %s", addr)
		state.setConnected(true)

		connDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				conn.Close()
			case <-connDone:
			}
		}()

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var w wireState
			if err := json.Unmarshal(scanner.Bytes(), &w); err != nil {
				continue
			}
			state.apply(w)
		}
		close(connDone)
		conn.Close()
		state.setConnected(false)

		if ctx.Err() != nil {
			return
		}
		log.Printf("[Telemetry] Disconnected, retrying...")
		if !sleepOrDone(ctx, 2*time.Second) {
			return
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
