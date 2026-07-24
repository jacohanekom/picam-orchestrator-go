package luxswitch

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDecideSwitch(t *testing.T) {
	const threshold = 50
	cases := []struct {
		name         string
		lux          float64
		activeCamera int
		want         int
	}{
		{"bright, above deadband, on camera 1 -> switch to 0", 60, 1, 0},
		{"bright, above deadband, already on camera 0 -> no change", 60, 0, -1},
		{"dark, below deadband, on camera 0 -> switch to 1", 40, 0, 1},
		{"dark, below deadband, already on camera 1 -> no change", 40, 1, -1},
		{"exactly at threshold -> within deadband, no change", 50, 1, -1},
		{"just inside the deadband above -> no change", float64(threshold + deadband - 1), 1, -1},
		{"just past the deadband above -> switch", float64(threshold + deadband + 1), 1, 0},
		{"just inside the deadband below -> no change", float64(threshold - deadband + 1), 0, -1},
		{"just past the deadband below -> switch", float64(threshold - deadband - 1), 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideSwitch(tc.lux, threshold, tc.activeCamera)
			if got != tc.want {
				t.Errorf("decideSwitch(%v, %d, %d) = %d, want %d", tc.lux, threshold, tc.activeCamera, got, tc.want)
			}
		})
	}
}

func TestStatePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lux_switch.json")

	s1 := New(path, false, 50)
	if enabled, threshold := s1.Get(); enabled != false || threshold != 50 {
		t.Fatalf("fresh state = (%v, %d), want (false, 50)", enabled, threshold)
	}

	enabled, threshold := true, 75
	s1.Set(&enabled, &threshold)

	// A new State constructed from the same path should load the
	// persisted values instead of the (now stale) defaults passed in.
	s2 := New(path, false, 50)
	if gotEnabled, gotThreshold := s2.Get(); gotEnabled != true || gotThreshold != 75 {
		t.Fatalf("reloaded state = (%v, %d), want (true, 75)", gotEnabled, gotThreshold)
	}
}

func TestStateSetPartialUpdate(t *testing.T) {
	s := New("", true, 50) // empty path -- persistence disabled, in-memory only

	newThreshold := 80
	s.Set(nil, &newThreshold)
	if enabled, threshold := s.Get(); enabled != true || threshold != 80 {
		t.Fatalf("after threshold-only Set = (%v, %d), want (true, 80)", enabled, threshold)
	}

	disabled := false
	s.Set(&disabled, nil)
	if enabled, threshold := s.Get(); enabled != false || threshold != 80 {
		t.Fatalf("after enabled-only Set = (%v, %d), want (false, 80)", enabled, threshold)
	}
}

func TestStateMissingFileFallsBackToDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	s := New(path, true, 42)
	if enabled, threshold := s.Get(); enabled != true || threshold != 42 {
		t.Fatalf("state with no persisted file = (%v, %d), want defaults (true, 42)", enabled, threshold)
	}
}

func TestCoolingDown(t *testing.T) {
	s := New("", false, 50)

	if s.coolingDown() {
		t.Fatalf("first call should not report cooling down (no prior switch)")
	}
	if !s.coolingDown() {
		t.Fatalf("immediately after a switch, should report cooling down")
	}

	s.undoLastSwitch()
	if s.coolingDown() {
		t.Fatalf("after undoLastSwitch, a failed attempt should not count toward the cooldown")
	}
}

func TestCoolingDownExpires(t *testing.T) {
	s := New("", false, 50)
	s.coolingDown() // consume the cooldown window
	s.mu.Lock()
	s.lastSwitchAt = time.Now().Add(-cooldown - time.Second) // simulate elapsed time
	s.mu.Unlock()

	if s.coolingDown() {
		t.Fatalf("cooldown should have expired")
	}
}
