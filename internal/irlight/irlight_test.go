package irlight

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDecideRelay(t *testing.T) {
	const threshold = 50
	cases := []struct {
		name         string
		lux          float64
		currentlyOn  bool
		onFor        time.Duration
		maxOnMinutes int
		cutoffArmed  bool
		wantOn       bool
		wantArmed    bool
	}{
		{"bright, currently off -> stays off", 60, false, 0, 0, false, false, false},
		{"bright, currently on -> turns off and disarms", 60, true, 0, 0, true, false, false},
		{"dark, not armed, currently off -> turns on", 40, false, 0, 0, false, true, false},
		{"dark, currently on -> stays on", 40, true, 0, 0, false, true, false},
		{"dark but cutoff armed -> stays off", 40, false, 0, 0, true, false, true},
		{"within deadband above -> no change (off stays off)", threshold + deadband - 1, false, 0, 0, false, false, false},
		{"within deadband below -> no change (on stays on)", threshold - deadband + 1, true, 0, 0, false, true, false},
		{"dark, no cap, long on-time -> stays on", 40, true, 999 * time.Minute, 0, false, true, false},
		{
			"dark, on-time exceeds cap -> forced off and armed",
			40, true, 31 * time.Minute, 30, false, false, true,
		},
		{
			"dark, on-time under cap -> stays on, not armed",
			40, true, 29 * time.Minute, 30, false, true, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOn, gotArmed := decideRelay(tc.lux, threshold, tc.currentlyOn, tc.onFor, tc.maxOnMinutes, tc.cutoffArmed)
			if gotOn != tc.wantOn || gotArmed != tc.wantArmed {
				t.Errorf("decideRelay(%v, %d, %v, %v, %d, %v) = (%v, %v), want (%v, %v)",
					tc.lux, threshold, tc.currentlyOn, tc.onFor, tc.maxOnMinutes, tc.cutoffArmed,
					gotOn, gotArmed, tc.wantOn, tc.wantArmed)
			}
		})
	}
}

func TestStatePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ir_light.json")

	s1 := New(path, false, 50, 0)
	if enabled, threshold, maxOn := s1.Get(); enabled != false || threshold != 50 || maxOn != 0 {
		t.Fatalf("fresh state = (%v, %d, %d), want (false, 50, 0)", enabled, threshold, maxOn)
	}

	enabled, threshold, maxOn := true, 75, 30
	s1.Set(&enabled, &threshold, &maxOn)

	// A new State constructed from the same path should load the
	// persisted values instead of the (now stale) defaults passed in.
	s2 := New(path, false, 50, 0)
	if gotEnabled, gotThreshold, gotMaxOn := s2.Get(); gotEnabled != true || gotThreshold != 75 || gotMaxOn != 30 {
		t.Fatalf("reloaded state = (%v, %d, %d), want (true, 75, 30)", gotEnabled, gotThreshold, gotMaxOn)
	}
}

func TestStateSetPartialUpdate(t *testing.T) {
	s := New("", true, 50, 10) // empty path -- persistence disabled, in-memory only

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

func TestStateMissingFileFallsBackToDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	s := New(path, true, 42, 15)
	if enabled, threshold, maxOn := s.Get(); enabled != true || threshold != 42 || maxOn != 15 {
		t.Fatalf("state with no persisted file = (%v, %d, %d), want defaults (true, 42, 15)", enabled, threshold, maxOn)
	}
}

func TestCoolingDown(t *testing.T) {
	s := New("", false, 50, 0)

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
	s := New("", false, 50, 0)
	s.coolingDown() // consume the cooldown window
	s.mu.Lock()
	s.lastToggleAt = time.Now().Add(-cooldown - time.Second) // simulate elapsed time
	s.mu.Unlock()

	if s.coolingDown() {
		t.Fatalf("cooldown should have expired")
	}
}

func TestRuntimeSnapshotAndCommitToggle(t *testing.T) {
	s := New("", false, 50, 0)

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
