// Package irlight implements IR-illuminator control via a relay (the
// sibling pi-relay-control daemon, running locally on this same Pi),
// driven by up to three independent sources that all share one relay:
//
//   - The "dark" trigger: below a configured lux threshold, the relay
//     is switched on; above it, off.
//   - The "sunrise" trigger: forces the relay on for a configurable
//     window of minutes before/after computed local sunrise
//     (github.com/nathan-osman/go-sunrise), regardless of the lux
//     reading — a guaranteed pre-dawn boost independent of the dark
//     trigger's own state.
//   - A manual trigger (Trigger), for direct on/off commands (e.g. a
//     UI button) independent of either automatic trigger. If either
//     automatic trigger is enabled, its next tick re-asserts control;
//     if both are disabled, the relay just stays as manually set.
//     Manual changes are also subject to their own longer cooldown
//     (manualCooldown, 3 minutes) on top of the shared one below --
//     purely to stop a human from rapidly re-toggling it in the UI,
//     independent of the automatic triggers' own responsiveness.
//
// A configurable cap limits how many minutes the relay may stay
// continuously on — regardless of which of the three sources put it
// there — as a hardware safety limit. Once hit (from the dark or
// manual trigger), the relay is forced off and stays off until lux
// rises back above the threshold (day) and drops below it again (a
// fresh dark session); the sunrise trigger is not subject to that
// arm/disarm state, since it's already a short, separately-bounded
// window.
//
// All settings are configured live over HTTP (see webrtcsrv's
// /ir-light and /ir-light/trigger handlers) and persisted to disk so
// they survive a service restart, exactly like internal/luxswitch.
package irlight

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nathan-osman/go-sunrise"

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

	// manualCooldown is a separate, longer cooldown specific to Trigger
	// (the manual on/off endpoint) -- distinct from cooldown above,
	// which also gates the automatic dark/sunrise triggers and is tuned
	// for their lux-crossing responsiveness. This one exists purely to
	// stop a human from rapidly flipping the manual control in the UI.
	manualCooldown = 3 * time.Minute
)

// State holds the live-configurable enabled/threshold/maxOnMinutes and
// sunrise-window settings (persisted to disk) plus runtime
// relay-tracking fields (never persisted — they reset on restart, same
// as luxswitch's own lastSwitchAt).
type State struct {
	mu                   sync.Mutex
	enabled              bool
	threshold            int
	maxOnMinutes         int
	sunriseEnabled       bool
	sunriseBeforeMinutes int
	sunriseAfterMinutes  int
	statePath            string

	relayOn            bool      // last commanded relay state
	onSince            time.Time // when the current ON streak began (zero if off)
	cutoffArmed        bool      // true once max-on-minutes has forced this dark session off
	lastToggleAt       time.Time // cooldown tracking, shared with the automatic triggers
	lastManualToggleAt time.Time // manualCooldown tracking, Trigger-only
}

type persisted struct {
	Enabled              bool `json:"enabled"`
	Threshold            int  `json:"threshold"`
	MaxOnMinutes         int  `json:"max_on_minutes"`
	SunriseEnabled       bool `json:"sunrise_enabled"`
	SunriseBeforeMinutes int  `json:"sunrise_before_minutes"`
	SunriseAfterMinutes  int  `json:"sunrise_after_minutes"`
}

// New builds a State seeded with the given defaults (normally read from
// config.ini), then overrides them from a previously persisted state
// file at statePath if one exists. statePath may be empty, in which
// case persistence is silently disabled.
func New(statePath string, defaultEnabled bool, defaultThreshold, defaultMaxOnMinutes int,
	defaultSunriseEnabled bool, defaultSunriseBeforeMinutes, defaultSunriseAfterMinutes int) *State {
	s := &State{
		statePath:            statePath,
		enabled:              defaultEnabled,
		threshold:            defaultThreshold,
		maxOnMinutes:         defaultMaxOnMinutes,
		sunriseEnabled:       defaultSunriseEnabled,
		sunriseBeforeMinutes: defaultSunriseBeforeMinutes,
		sunriseAfterMinutes:  defaultSunriseAfterMinutes,
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
	s.sunriseEnabled = p.SunriseEnabled
	s.sunriseBeforeMinutes = p.SunriseBeforeMinutes
	s.sunriseAfterMinutes = p.SunriseAfterMinutes
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
	data, err := json.Marshal(persisted{
		Enabled:              s.enabled,
		Threshold:            s.threshold,
		MaxOnMinutes:         s.maxOnMinutes,
		SunriseEnabled:       s.sunriseEnabled,
		SunriseBeforeMinutes: s.sunriseBeforeMinutes,
		SunriseAfterMinutes:  s.sunriseAfterMinutes,
	})
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

// GetSunrise returns the current sunrise-window settings.
func (s *State) GetSunrise() (enabled bool, beforeMinutes, afterMinutes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sunriseEnabled, s.sunriseBeforeMinutes, s.sunriseAfterMinutes
}

// SetSunrise updates whichever of enabled/beforeMinutes/afterMinutes is
// non-nil, leaving the others unchanged, persists the result, and
// returns the resulting settings. Same "absent param = no change"
// convention as Set.
func (s *State) SetSunrise(enabled *bool, beforeMinutes, afterMinutes *int) (enabledOut bool, beforeOut, afterOut int) {
	s.mu.Lock()
	if enabled != nil {
		s.sunriseEnabled = *enabled
	}
	if beforeMinutes != nil {
		s.sunriseBeforeMinutes = *beforeMinutes
	}
	if afterMinutes != nil {
		s.sunriseAfterMinutes = *afterMinutes
	}
	enabledOut, beforeOut, afterOut = s.sunriseEnabled, s.sunriseBeforeMinutes, s.sunriseAfterMinutes
	s.mu.Unlock()
	s.save()
	return enabledOut, beforeOut, afterOut
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

// RelayOn reports the last-commanded relay state, for exposing over
// /status.json -- lets a client (e.g. the manual-trigger toggle in
// picam-frontend's Settings page) confirm a trigger actually took
// effect, or track a change made by either automatic trigger, without
// needing its own copy of the decision logic.
func (s *State) RelayOn() bool {
	relayOn, _, _ := s.runtimeSnapshot()
	return relayOn
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

// manualCoolingDown reports whether Trigger last successfully changed
// the relay too recently to allow another manual change yet, and if
// so, how much longer the caller must wait. Unlike coolingDown, this
// is a pure read with no side effect: lastManualToggleAt is only ever
// set by markManualToggle, after a change has actually gone through,
// so there's nothing to undo on a blocked or failed attempt.
func (s *State) manualCoolingDown() (blocked bool, retryAfter time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastManualToggleAt.IsZero() {
		return false, 0
	}
	elapsed := time.Since(s.lastManualToggleAt)
	if elapsed >= manualCooldown {
		return false, 0
	}
	return true, manualCooldown - elapsed
}

// markManualToggle records a successfully-applied manual toggle, for
// manualCoolingDown's cooldown tracking.
func (s *State) markManualToggle() {
	s.mu.Lock()
	s.lastManualToggleAt = time.Now()
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

// decideDarkTrigger computes whether the lux-based "dark" trigger wants
// the relay on, and whether the dark-session cutoff should be armed or
// disarmed, given the current lux reading, threshold, on/off state, and
// whether the cutoff was already armed. Does NOT apply the
// max-on-minutes cap — see applyMaxOnCap, which is applied once to the
// combined decision from both triggers (see tick). Pure and
// side-effect free so it's cheap to unit test independently of
// telemetry/network state, mirroring luxswitch.decideSwitch.
func decideDarkTrigger(lux float64, threshold int, currentlyOn, cutoffArmed bool) (wantOn, nextCutoffArmed bool) {
	switch {
	case lux > float64(threshold+deadband):
		return false, false // bright: off, and re-arm for the next dark session
	case lux < float64(threshold-deadband):
		return !cutoffArmed, cutoffArmed
	default:
		return currentlyOn, cutoffArmed // within the deadband: no change
	}
}

// sunriseWindowActive reports whether now falls within
// [sunrise-beforeMinutes, sunrise+afterMinutes] for the given day's
// computed sunrise. A zero sunriseTime (e.g. polar day/night, per
// go-sunrise's own contract) is treated as inactive. Pure given a
// sunrise time, so the astronomical calculation itself stays outside
// this decision logic and this stays cheaply unit-testable.
func sunriseWindowActive(now, sunriseTime time.Time, beforeMinutes, afterMinutes int) bool {
	if sunriseTime.IsZero() {
		return false
	}
	start := sunriseTime.Add(-time.Duration(beforeMinutes) * time.Minute)
	end := sunriseTime.Add(time.Duration(afterMinutes) * time.Minute)
	return !now.Before(start) && now.Before(end)
}

// applyMaxOnCap enforces the hardware safety cap over the combined
// (dark-trigger OR sunrise-trigger) decision, independent of which
// trigger asked for the relay to be on. maxOnMinutes <= 0 means no cap.
func applyMaxOnCap(wantOn bool, onFor time.Duration, maxOnMinutes int) (nextWantOn, cutoff bool) {
	if wantOn && maxOnMinutes > 0 && onFor >= time.Duration(maxOnMinutes)*time.Minute {
		return false, true
	}
	return wantOn, false
}

// Run evaluates both triggers every pollInterval and switches the relay
// accordingly (with deadband + cooldown + the max-on-minutes safety
// cap), until ctx is cancelled. lat/lon locate this Pi for the sunrise
// trigger's computation.
func Run(ctx context.Context, state *State, tel *telemetry.State, relayHost string, relayPort int, lat, lon float64) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick(state, tel, relayHost, relayPort, lat, lon)
		}
	}
}

// Trigger directly commands the relay on/off, bypassing the dark/
// sunrise decision logic entirely — but still through the same
// cooldown and runtime tracking (onSince) as an automatic toggle, so
// the max-on-minutes safety cap and relay-wear cooldown apply to a
// manually-triggered state exactly as they would to an automatic one.
// The next tick will re-assert automatic control if either trigger is
// enabled; if both are disabled, the relay simply stays as set here.
func Trigger(state *State, relayHost string, relayPort int, on bool) (reached bool, retryAfter time.Duration) {
	current, _, _ := state.runtimeSnapshot()
	if on == current {
		return true, 0 // already in the requested state, nothing to do
	}
	if blocked, wait := state.manualCoolingDown(); blocked {
		return false, wait
	}
	if state.coolingDown() {
		return false, 0
	}
	reached, _ = relayrpc.SetRelay(relayHost, relayPort, on)
	if !reached {
		state.undoLastToggle()
		return false, 0
	}
	state.commitToggle(on)
	state.markManualToggle()
	return true, 0
}

func tick(state *State, tel *telemetry.State, relayHost string, relayPort int, lat, lon float64) {
	enabled, threshold, maxOnMinutes := state.Get()
	sunriseEnabled, beforeMin, afterMin := state.GetSunrise()

	currentlyOn, onFor, cutoffArmed := state.runtimeSnapshot()
	// Default: no automatic trigger has an opinion this tick -- hold
	// whatever's already there (e.g. a manually-triggered state). The
	// cap check below still applies regardless, so a manual "on" is
	// never exempt from the hardware safety limit.
	want, nextArmed := currentlyOn, cutoffArmed

	if enabled {
		if snap := tel.Snapshot(); snap.Connected {
			want, nextArmed = decideDarkTrigger(float64(snap.Lux), threshold, currentlyOn, cutoffArmed)
		} else {
			// Telemetry down: the dark trigger defers to whatever the
			// relay is already doing rather than forcing a change.
			want = currentlyOn
		}
	}

	if sunriseEnabled {
		now := time.Now()
		rise, _ := sunrise.SunriseSunset(lat, lon, now.Year(), now.Month(), now.Day())
		if sunriseWindowActive(now, rise, beforeMin, afterMin) {
			// The sunrise window always wins if active, regardless of
			// the dark trigger's own cutoff-armed state -- it's a
			// short, separately-bounded, deliberately-scheduled
			// window, not part of that dark session's allotment.
			want = true
		}
	}

	if capped, cutoff := applyMaxOnCap(want, onFor, maxOnMinutes); cutoff {
		want, nextArmed = capped, true
	} else {
		want = capped
	}

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
	log.Printf("[IRLight] relay -> %s (dark_enabled=%v sunrise_enabled=%v)", onOff, enabled, sunriseEnabled)
}
