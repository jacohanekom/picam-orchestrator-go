package irlight

import (
	"path/filepath"
	"testing"
	"time"

	"picam-orchestrator/internal/telemetry"
)

func TestDecideDarkTrigger(t *testing.T) {
	const threshold = 50
	cases := []struct {
		name        string
		lux         float64
		currentlyOn bool
		cutoffArmed bool
		wantOn      bool
		wantArmed   bool
	}{
		{"bright, currently off -> stays off", 60, false, false, false, false},
		{"bright, currently on -> turns off and disarms", 60, true, true, false, false},
		{"dark, not armed, currently off -> turns on", 40, false, false, true, false},
		{"dark, currently on -> stays on", 40, true, false, true, false},
		{"dark but cutoff armed -> stays off", 40, false, true, false, true},
		{"within deadband above -> no change (off stays off)", threshold + deadband - 1, false, false, false, false},
		{"within deadband below -> no change (on stays on)", threshold - deadband + 1, true, false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOn, gotArmed := decideDarkTrigger(tc.lux, threshold, tc.currentlyOn, tc.cutoffArmed)
			if gotOn != tc.wantOn || gotArmed != tc.wantArmed {
				t.Errorf("decideDarkTrigger(%v, %d, %v, %v) = (%v, %v), want (%v, %v)",
					tc.lux, threshold, tc.currentlyOn, tc.cutoffArmed, gotOn, gotArmed, tc.wantOn, tc.wantArmed)
			}
		})
	}
}

func TestSunriseWindowActive(t *testing.T) {
	sunrise := time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC)
	const before, after = 30, 15

	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"well before window", sunrise.Add(-45 * time.Minute), false},
		{"exactly at window start", sunrise.Add(-30 * time.Minute), true},
		{"inside window before sunrise", sunrise.Add(-10 * time.Minute), true},
		{"exactly at sunrise", sunrise, true},
		{"inside window after sunrise", sunrise.Add(10 * time.Minute), true},
		{"exactly at window end", sunrise.Add(15 * time.Minute), false},
		{"well after window", sunrise.Add(30 * time.Minute), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sunriseWindowActive(tc.now, sunrise, before, after); got != tc.want {
				t.Errorf("sunriseWindowActive(%v, %v, %d, %d) = %v, want %v", tc.now, sunrise, before, after, got, tc.want)
			}
		})
	}

	t.Run("zero sunrise time is always inactive", func(t *testing.T) {
		if sunriseWindowActive(sunrise, time.Time{}, before, after) {
			t.Errorf("expected inactive for a zero sunrise time (e.g. polar day/night)")
		}
	})
}

func TestApplyMaxOnCap(t *testing.T) {
	cases := []struct {
		name         string
		wantOn       bool
		onFor        time.Duration
		maxOnMinutes int
		nextWantOn   bool
		cutoff       bool
	}{
		{"already off -> no-op regardless of onFor", false, 999 * time.Minute, 30, false, false},
		{"no cap -> stays on however long", true, 999 * time.Minute, 0, true, false},
		{"under cap -> stays on, no cutoff", true, 29 * time.Minute, 30, true, false},
		{"exactly at cap -> forced off and cutoff", true, 30 * time.Minute, 30, false, true},
		{"over cap -> forced off and cutoff", true, 31 * time.Minute, 30, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotWant, gotCutoff := applyMaxOnCap(tc.wantOn, tc.onFor, tc.maxOnMinutes)
			if gotWant != tc.nextWantOn || gotCutoff != tc.cutoff {
				t.Errorf("applyMaxOnCap(%v, %v, %d) = (%v, %v), want (%v, %v)",
					tc.wantOn, tc.onFor, tc.maxOnMinutes, gotWant, gotCutoff, tc.nextWantOn, tc.cutoff)
			}
		})
	}
}

func TestStatePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ir_light.json")

	s1 := New(path, false, 50, 0, false, 30, 15)
	enabled, threshold, maxOn := s1.Get()
	if enabled != false || threshold != 50 || maxOn != 0 {
		t.Fatalf("fresh state Get() = (%v, %d, %d), want (false, 50, 0)", enabled, threshold, maxOn)
	}
	sunEnabled, before, after := s1.GetSunrise()
	if sunEnabled != false || before != 30 || after != 15 {
		t.Fatalf("fresh state GetSunrise() = (%v, %d, %d), want (false, 30, 15)", sunEnabled, before, after)
	}

	newEnabled, newThreshold, newMaxOn := true, 75, 30
	s1.Set(&newEnabled, &newThreshold, &newMaxOn)
	newSunEnabled, newBefore, newAfter := true, 45, 20
	s1.SetSunrise(&newSunEnabled, &newBefore, &newAfter)

	// A new State constructed from the same path should load the
	// persisted values instead of the (now stale) defaults passed in.
	s2 := New(path, false, 50, 0, false, 30, 15)
	if gotEnabled, gotThreshold, gotMaxOn := s2.Get(); gotEnabled != true || gotThreshold != 75 || gotMaxOn != 30 {
		t.Fatalf("reloaded Get() = (%v, %d, %d), want (true, 75, 30)", gotEnabled, gotThreshold, gotMaxOn)
	}
	if gotSunEnabled, gotBefore, gotAfter := s2.GetSunrise(); gotSunEnabled != true || gotBefore != 45 || gotAfter != 20 {
		t.Fatalf("reloaded GetSunrise() = (%v, %d, %d), want (true, 45, 20)", gotSunEnabled, gotBefore, gotAfter)
	}
}

func TestStateSetPartialUpdate(t *testing.T) {
	s := New("", true, 50, 10, true, 30, 15) // empty path -- persistence disabled, in-memory only

	newThreshold := 80
	s.Set(nil, &newThreshold, nil)
	if enabled, threshold, maxOn := s.Get(); enabled != true || threshold != 80 || maxOn != 10 {
		t.Fatalf("after threshold-only Set = (%v, %d, %d), want (true, 80, 10)", enabled, threshold, maxOn)
	}

	disabled := false
	s.Set(&disabled, nil, nil)
	if enabled, threshold, maxOn := s.Get(); enabled != false || threshold != 80 || maxOn != 10 {
		t.Fatalf("after enabled-only Set = (%v, %d, %d), want (false, 80, 10)", enabled, threshold, maxOn)
	}
}

func TestStateSetSunrisePartialUpdate(t *testing.T) {
	s := New("", false, 50, 0, true, 30, 15)

	newBefore := 40
	s.SetSunrise(nil, &newBefore, nil)
	if enabled, before, after := s.GetSunrise(); enabled != true || before != 40 || after != 15 {
		t.Fatalf("after before-only SetSunrise = (%v, %d, %d), want (true, 40, 15)", enabled, before, after)
	}

	disabled := false
	s.SetSunrise(&disabled, nil, nil)
	if enabled, before, after := s.GetSunrise(); enabled != false || before != 40 || after != 15 {
		t.Fatalf("after enabled-only SetSunrise = (%v, %d, %d), want (false, 40, 15)", enabled, before, after)
	}
}

func TestStateMissingFileFallsBackToDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	s := New(path, true, 42, 15, true, 25, 10)
	if enabled, threshold, maxOn := s.Get(); enabled != true || threshold != 42 || maxOn != 15 {
		t.Fatalf("state with no persisted file Get() = (%v, %d, %d), want defaults (true, 42, 15)", enabled, threshold, maxOn)
	}
	if sunEnabled, before, after := s.GetSunrise(); sunEnabled != true || before != 25 || after != 10 {
		t.Fatalf("state with no persisted file GetSunrise() = (%v, %d, %d), want defaults (true, 25, 10)", sunEnabled, before, after)
	}
}

func TestCoolingDown(t *testing.T) {
	s := New("", false, 50, 0, false, 30, 15)

	if s.coolingDown() {
		t.Fatalf("first call should not report cooling down (no prior toggle)")
	}
	if !s.coolingDown() {
		t.Fatalf("immediately after a toggle, should report cooling down")
	}

	s.undoLastToggle()
	if s.coolingDown() {
		t.Fatalf("after undoLastToggle, a failed attempt should not count toward the cooldown")
	}
}

func TestCoolingDownExpires(t *testing.T) {
	s := New("", false, 50, 0, false, 30, 15)
	s.coolingDown() // consume the cooldown window
	s.mu.Lock()
	s.lastToggleAt = time.Now().Add(-cooldown - time.Second) // simulate elapsed time
	s.mu.Unlock()

	if s.coolingDown() {
		t.Fatalf("cooldown should have expired")
	}
}

func TestRuntimeSnapshotAndCommitToggle(t *testing.T) {
	s := New("", false, 50, 0, false, 30, 15)

	on, onFor, armed := s.runtimeSnapshot()
	if on || onFor != 0 || armed {
		t.Fatalf("fresh state runtimeSnapshot = (%v, %v, %v), want (false, 0, false)", on, onFor, armed)
	}

	s.commitToggle(true)
	on, onFor, armed = s.runtimeSnapshot()
	if !on || onFor < 0 {
		t.Fatalf("after commitToggle(true), runtimeSnapshot = (%v, %v, %v), want on=true, onFor>=0", on, onFor, armed)
	}

	s.commitToggle(false)
	on, onFor, armed = s.runtimeSnapshot()
	if on || onFor != 0 {
		t.Fatalf("after commitToggle(false), runtimeSnapshot = (%v, %v, %v), want (false, 0, ...)", on, onFor, armed)
	}
}

func TestTrigger(t *testing.T) {
	t.Run("already in requested state is a no-op", func(t *testing.T) {
		s := New("", false, 50, 0, false, 30, 15) // relay starts off
		// Deliberately empty host:port -- if this reached the network at
		// all, relayrpc.SetRelay would fail and Trigger would return false.
		if !Trigger(s, "", 0, false) {
			t.Fatalf("Trigger(off) on an already-off relay should be a no-op returning true")
		}
	})

	t.Run("cooldown blocks a toggle attempted too soon after another", func(t *testing.T) {
		s := New("", false, 50, 0, false, 30, 15)
		s.mu.Lock()
		s.lastToggleAt = time.Now() // simulate a toggle that just happened
		s.mu.Unlock()

		if Trigger(s, "", 0, true) {
			t.Fatalf("Trigger should be blocked by an active cooldown")
		}
		on, _, _ := s.runtimeSnapshot()
		if on {
			t.Fatalf("a cooldown-blocked Trigger should not change the tracked relay state")
		}
	})

	t.Run("a failed relay command undoes the cooldown so it can be retried", func(t *testing.T) {
		s := New("", false, 50, 0, false, 30, 15)
		if Trigger(s, "127.0.0.1", 1, true) { // port 1 -- nothing listens there
			t.Fatalf("Trigger against an unreachable relay should report failure")
		}
		s.mu.Lock()
		zero := s.lastToggleAt.IsZero()
		s.mu.Unlock()
		if !zero {
			t.Fatalf("a failed Trigger should undo the cooldown timestamp so the next attempt isn't blocked")
		}
	})
}

// TestTickAppliesCapEvenWithBothTriggersDisabled closes the gap Trigger
// introduces: a manually-triggered "on" state must still be subject to
// the max-on-minutes hardware safety cap, even though neither
// automatic trigger is enabled to otherwise drive tick's decision.
func TestTickAppliesCapEvenWithBothTriggersDisabled(t *testing.T) {
	s := New("", false, 50, 0, false, 30, 15) // both triggers disabled
	s.commitToggle(true)                      // simulate a manually-triggered "on" state
	s.mu.Lock()
	s.onSince = time.Now().Add(-31 * time.Minute) // pretend it's been on 31 minutes
	s.maxOnMinutes = 30
	s.mu.Unlock()

	tel := &telemetry.State{} // zero value: disconnected, irrelevant since both triggers are disabled

	// Unreachable relay: tick will attempt to command it off (the cap
	// firing) and that attempt will fail, but the cutoff-armed bookkeeping
	// happens before that attempt regardless of whether it succeeds.
	tick(s, tel, "127.0.0.1", 1, 0, 0)

	_, _, armed := s.runtimeSnapshot()
	if !armed {
		t.Fatalf("expected the safety cap to arm the cutoff even with both automatic triggers disabled")
	}
}
