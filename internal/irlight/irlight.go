// Package irlight implements automatic IR-illuminator control based on
// ambient light: below a configured lux threshold, a relay wired to the
// light (via the sibling pi-relay-control daemon, running locally on
// this same Pi) is switched on; above it, off. A configurable cap
// limits how many minutes the relay may stay continuously on, as a
// hardware safety limit — once hit, the relay stays off for the rest
// of that dark period and only re-arms once lux rises back above the
// threshold (day) and drops below it again (a fresh dark session).
// The enabled flag, threshold, and cap are configured live over HTTP
// (see webrtcsrv's /ir-light handler) and persisted to disk so they
// survive a service restart, exactly like internal/luxswitch.
package irlight

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"picam-orchestrator/internal/relayrpc"
	"picam-orchestrator/internal/telemetry"
)

// deadband and cooldown mirror internal/luxswitch's own constants
// exactly — same reasoning: avoid flapping right at the threshold, and
// protect the physical relay from chatter on a noisy lux reading.
const (
	deadband     = 5
	cooldown     = 30 * time.Second
	pollInterval = 5 * time.Second
)

// State holds the live-configurable enabled/threshold/maxOnMinutes
// settings (persisted to disk) plus runtime relay-tracking fields
// (never persisted — they reset on restart, same as luxswitch's own
// lastSwitchAt).
type State struct {
	mu           sync.Mutex
	enabled      bool
	threshold    int
	maxOnMinutes int
	statePath    string

	relayOn      bool      // last commanded relay state
	onSince      time.Time // when the current ON streak began (zero if off)
	cutoffArmed  bool      // true once max-on-minutes has forced this dark session off
	lastToggleAt time.Time // cooldown tracking
}

type persisted struct {
	Enabled      bool `json:"enabled"`
	Threshold    int  `json:"threshold"`
	MaxOnMinutes int  `json:"max_on_minutes"`
}

// New builds a State seeded with the given defaults (normally read from
// config.ini), then overrides them from a previously persisted state
// file at statePath if one exists. statePath may be empty, in which
// case persistence is silently disabled.
func New(statePath string, defaultEnabled bool, defaultThreshold, defaultMaxOnMinutes int) *State {
	s := &State{
		statePath:    statePath,
		enabled:      defaultEnabled,
		threshold:    defaultThreshold,
		maxOnMinutes: defaultMaxOnMinutes,
	}
	s.load()
	return s
}

// load overwrites the seeded defaults from disk. A missing or corrupt
// file is not an error — the caller's defaults stand, same tolerance
// as luxswitch.State.load.
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
		log.Printf("[IRLight] ignoring corrupt state file %s: %v", s.statePath, err)
		return
	}
	s.enabled = p.Enabled
	s.threshold = p.Threshold
	s.maxOnMinutes = p.MaxOnMinutes
}

// save writes the current settings to disk. A failure here is logged
// and swallowed, not propagated — persistence is a nice-to-have, not a
// boot-critical dependency.
func (s *State) save() {
	if s.statePath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		log.Printf("[IRLight] failed to create state dir for %s: %v", s.statePath, err)
		return
	}
	data, err := json.Marshal(persisted{Enabled: s.enabled, Threshold: s.threshold, MaxOnMinutes: s.maxOnMinutes})
	if err != nil {
		return
	}
	if err := os.WriteFile(s.statePath, data, 0o644); err != nil {
		log.Printf("[IRLight] failed to persist state to %s: %v", s.statePath, err)
	}
}

// Get returns the current enabled/threshold/maxOnMinutes settings.
func (s *State) Get() (enabled bool, threshold, maxOnMinutes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled, s.threshold, s.maxOnMinutes
}

// Set updates whichever of enabled/threshold/maxOnMinutes is non-nil,
// leaving the others unchanged (same "absent param = no change"
// convention as /osd, /annotate, and /lux-switch), persists the
// result, and returns the resulting settings.
func (s *State) Set(enabled *bool, threshold, maxOnMinutes *int) (enabledOut bool, thresholdOut, maxOnMinutesOut int) {
	s.mu.Lock()
	if enabled != nil {
		s.enabled = *enabled
	}
	if threshold != nil {
		s.threshold = *threshold
	}
	if maxOnMinutes != nil {
		s.maxOnMinutes = *maxOnMinutes
	}
	enabledOut, thresholdOut, maxOnMinutesOut = s.enabled, s.threshold, s.maxOnMinutes
	s.mu.Unlock()
	s.save()
	return enabledOut, thresholdOut, maxOnMinutesOut
}

// runtimeSnapshot returns the current relay-tracking fields for use by
// tick, without exposing the lock.
func (s *State) runtimeSnapshot() (relayOn bool, onFor time.Duration, cutoffArmed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.relayOn && !s.onSince.IsZero() {
		onFor = time.Since(s.onSince)
	}
	return s.relayOn, onFor, s.cutoffArmed
}

func (s *State) setCutoffArmed(armed bool) {
	s.mu.Lock()
	s.cutoffArmed = armed
	s.mu.Unlock()
}

// coolingDown reports whether a toggle happened too recently to
// trigger another one yet, and — if not — marks now as the last
// toggle time. Same combined check-and-set pattern as
// luxswitch.State.coolingDown.
func (s *State) coolingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastToggleAt) < cooldown {
		return true
	}
	s.lastToggleAt = time.Now()
	return false
}

// undoLastToggle reverts the cooldown timer after a failed relay
// command, so a transient failure is retried next tick instead of
// silently blocking for the full cooldown window.
func (s *State) undoLastToggle() {
	s.mu.Lock()
	s.lastToggleAt = time.Time{}
	s.mu.Unlock()
}

// commitToggle records a successfully-applied relay state change.
func (s *State) commitToggle(on bool) {
	s.mu.Lock()
	s.relayOn = on
	if on {
		s.onSince = time.Now()
	} else {
		s.onSince = time.Time{}
	}
	s.mu.Unlock()
}

// decideRelay computes whether the relay should be on and whether the
// dark-session cutoff should be armed, given the current lux reading,
// threshold, on/off state, how long it's been continuously on, the
// configured max-on-minutes cap (0 = no cap), and whether the cutoff
// was already armed earlier this dark session. Pure and side-effect
// free so it's cheap to unit test independently of telemetry/network
// state, mirroring luxswitch.decideSwitch.
func decideRelay(lux float64, threshold int, currentlyOn bool, onFor time.Duration, maxOnMinutes int, cutoffArmed bool) (wantOn, nextCutoffArmed bool) {
	switch {
	case lux > float64(threshold+deadband):
		return false, false // bright: off, and re-arm for the next dark session
	case lux < float64(threshold-deadband):
		wantOn = !cutoffArmed
	default:
		wantOn = currentlyOn // within the deadband: no change
	}
	if wantOn && maxOnMinutes > 0 && onFor >= time.Duration(maxOnMinutes)*time.Minute {
		return false, true // hit the safety cap -- force off and arm the cutoff
	}
	return wantOn, cutoffArmed
}

// Run evaluates the current lux reading against the configured
// threshold every pollInterval and switches the relay accordingly
// (with deadband + cooldown + the max-on-minutes safety cap), until
// ctx is cancelled.
func Run(ctx context.Context, state *State, tel *telemetry.State, relayHost string, relayPort int) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick(state, tel, relayHost, relayPort)
		}
	}
}

func tick(state *State, tel *telemetry.State, relayHost string, relayPort int) {
	enabled, threshold, maxOnMinutes := state.Get()
	if !enabled {
		return
	}
	snap := tel.Snapshot()
	if !snap.Connected {
		return
	}

	currentlyOn, onFor, cutoffArmed := state.runtimeSnapshot()
	want, nextArmed := decideRelay(float64(snap.Lux), threshold, currentlyOn, onFor, maxOnMinutes, cutoffArmed)
	if nextArmed != cutoffArmed {
		state.setCutoffArmed(nextArmed)
	}

	if want == currentlyOn {
		return
	}
	if state.coolingDown() {
		return
	}

	reached, _ := relayrpc.SetRelay(relayHost, relayPort, want)
	if !reached {
		state.undoLastToggle()
		return
	}
	state.commitToggle(want)
	onOff := "off"
	if want {
		onOff = "on"
	}
	log.Printf("[IRLight] relay -> %s (lux=%.1f, threshold=%d)", onOff, snap.Lux, threshold)
}
