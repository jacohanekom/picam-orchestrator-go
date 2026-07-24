// Package luxswitch implements automatic camera-lens switching based on
// ambient light: above a configured lux threshold picam-raw is told to
// use camera 0, below it camera 1. The enabled flag and threshold are
// configured live over HTTP (see webrtcsrv's /lux-switch handler) and
// persisted to disk so they survive a service restart, unlike this
// process's other runtime-toggleable settings (OSD, annotate), which are
// deliberately in-memory-only.
package luxswitch

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"picam-orchestrator/internal/camrpc"
	"picam-orchestrator/internal/telemetry"
)

// deadband keeps the switch from flapping right at the threshold: no
// action is taken while lux is within this many units of it either way.
// cooldown is the minimum time between two auto-triggered switches,
// separate from and in addition to the deadband — it also covers the
// case of a noisy/oscillating lux reading that repeatedly crosses the
// (deadband-widened) threshold in quick succession.
const (
	deadband     = 5
	cooldown     = 30 * time.Second
	pollInterval = 5 * time.Second
)

// State holds the live-configurable enabled/threshold settings, guarded
// by a mutex since they're read by the Run loop and written from an HTTP
// handler concurrently.
type State struct {
	mu           sync.Mutex
	enabled      bool
	threshold    int
	statePath    string
	lastSwitchAt time.Time
}

type persisted struct {
	Enabled   bool `json:"enabled"`
	Threshold int  `json:"threshold"`
}

// New builds a State seeded with the given defaults (normally read from
// config.ini), then overrides them from a previously persisted state
// file at statePath if one exists. statePath may be empty, in which case
// persistence is silently disabled (defaults are used, Set never writes
// to disk) — useful for tests or a deliberately stateless run.
func New(statePath string, defaultEnabled bool, defaultThreshold int) *State {
	s := &State{statePath: statePath, enabled: defaultEnabled, threshold: defaultThreshold}
	s.load()
	return s
}

// load overwrites the seeded defaults from disk. A missing or corrupt
// file is not an error — it just means no persisted value exists yet,
// so the caller's defaults stand, same tolerance as config.go's own
// missing-key handling.
func (s *State) load() {
	if s.statePath == "" {
		return
	}
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		return
	}
	var p persisted
	if err := json.Unmarshal(data, &p); err != nil {
		log.Printf("[LuxSwitch] ignoring corrupt state file %s: %v", s.statePath, err)
		return
	}
	s.enabled = p.Enabled
	s.threshold = p.Threshold
}

// save writes the current settings to disk. Persistence is a
// nice-to-have, not a boot-critical dependency, so a failure here is
// logged and swallowed rather than propagated — the in-memory setting
// the caller just applied still takes effect either way.
func (s *State) save() {
	if s.statePath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		log.Printf("[LuxSwitch] failed to create state dir for %s: %v", s.statePath, err)
		return
	}
	data, err := json.Marshal(persisted{Enabled: s.enabled, Threshold: s.threshold})
	if err != nil {
		return
	}
	if err := os.WriteFile(s.statePath, data, 0o644); err != nil {
		log.Printf("[LuxSwitch] failed to persist state to %s: %v", s.statePath, err)
	}
}

// Get returns the current enabled/threshold settings.
func (s *State) Get() (enabled bool, threshold int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled, s.threshold
}

// Set updates whichever of enabled/threshold is non-nil, leaving the
// other unchanged (same "absent param = no change" convention as the
// existing /osd and /annotate handlers), persists the result, and
// returns the resulting settings.
func (s *State) Set(enabled *bool, threshold *int) (enabledOut bool, thresholdOut int) {
	s.mu.Lock()
	if enabled != nil {
		s.enabled = *enabled
	}
	if threshold != nil {
		s.threshold = *threshold
	}
	enabledOut, thresholdOut = s.enabled, s.threshold
	s.mu.Unlock()
	s.save()
	return enabledOut, thresholdOut
}

// coolingDown reports whether an auto-triggered switch happened too
// recently to trigger another one yet, and — if not — marks now as the
// last switch time. Combined check-and-set under one lock so two ticks
// can't both decide the cooldown has elapsed and both fire.
func (s *State) coolingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastSwitchAt) < cooldown {
		return true
	}
	s.lastSwitchAt = time.Now()
	return false
}

// undoLastSwitch reverts the cooldown timer after a failed switch
// attempt, so a transient RPC failure gets retried next tick instead of
// silently blocking auto-switch for the full cooldown window.
func (s *State) undoLastSwitch() {
	s.mu.Lock()
	s.lastSwitchAt = time.Time{}
	s.mu.Unlock()
}

// Run evaluates the current lux reading against the configured
// threshold every pollInterval and switches picam-raw's active camera
// when it crosses (with deadband + cooldown to avoid flapping), until
// ctx is cancelled.
func Run(ctx context.Context, state *State, tel *telemetry.State, host string, cmdPort int) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick(state, tel, host, cmdPort)
		}
	}
}

// decideSwitch returns the camera index lux/threshold calls for given
// the current active camera, or -1 if no switch is warranted — either
// because lux is within the deadband of the threshold, or the wanted
// camera is already active. Pure and side-effect free so it's cheap to
// unit test independently of telemetry/network state.
func decideSwitch(lux float64, threshold, activeCamera int) int {
	want := -1
	switch {
	case lux > float64(threshold+deadband):
		want = 0
	case lux < float64(threshold-deadband):
		want = 1
	}
	if want == -1 || want == activeCamera {
		return -1
	}
	return want
}

func tick(state *State, tel *telemetry.State, host string, cmdPort int) {
	enabled, threshold := state.Get()
	if !enabled {
		return
	}
	snap := tel.Snapshot()
	if !snap.Connected {
		return
	}

	want := decideSwitch(float64(snap.Lux), threshold, snap.ActiveCamera)
	if want == -1 {
		return
	}
	if state.coolingDown() {
		return
	}

	reached, _ := camrpc.SwitchCamera(host, cmdPort, want)
	if !reached {
		state.undoLastSwitch()
		return
	}
	log.Printf("[LuxSwitch] switching camera %d -> %d (lux=%.1f, threshold=%d)", snap.ActiveCamera, want, snap.Lux, threshold)
}
