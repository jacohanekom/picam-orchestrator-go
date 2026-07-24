package uistate

import (
	"path/filepath"
	"testing"
)

func TestStatePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ui_state.json")

	s1 := New(path, false, false, false, false)
	cameraID, timeVal, main, lores, camera := true, true, true, false, 1
	s1.SetOSD(&cameraID, &timeVal)
	s1.SetAnnotate(&main, &lores)
	s1.SetActiveCamera(camera)

	// A new State constructed from the same path should load the
	// persisted values instead of the (now stale) defaults passed in.
	s2 := New(path, false, false, false, false)
	gotCameraID, gotTime, gotMain, gotLores, gotCamera := s2.Snapshot()
	if gotCameraID != true || gotTime != true || gotMain != true || gotLores != false || gotCamera != 1 {
		t.Fatalf("reloaded state = (%v, %v, %v, %v, %d), want (true, true, true, false, 1)",
			gotCameraID, gotTime, gotMain, gotLores, gotCamera)
	}
}

func TestSetOSDPartialUpdate(t *testing.T) {
	s := New("", true, true, false, false) // empty path -- persistence disabled, in-memory only

	timeVal := false
	s.SetOSD(nil, &timeVal)
	cameraID, gotTime, _, _, _ := s.Snapshot()
	if cameraID != true || gotTime != false {
		t.Fatalf("after time-only SetOSD = (cameraID=%v, time=%v), want (true, false)", cameraID, gotTime)
	}
}

func TestSetAnnotatePartialUpdate(t *testing.T) {
	s := New("", false, false, true, true)

	lores := false
	s.SetAnnotate(nil, &lores)
	_, _, main, gotLores, _ := s.Snapshot()
	if main != true || gotLores != false {
		t.Fatalf("after lores-only SetAnnotate = (main=%v, lores=%v), want (true, false)", main, gotLores)
	}
}

func TestStateMissingFileFallsBackToDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	s := New(path, true, false, true, false)
	cameraID, timeVal, main, lores, camera := s.Snapshot()
	if cameraID != true || timeVal != false || main != true || lores != false || camera != NoCameraPreference {
		t.Fatalf("state with no persisted file = (%v, %v, %v, %v, %d), want defaults + NoCameraPreference",
			cameraID, timeVal, main, lores, camera)
	}
}

func TestDecideCameraRestore(t *testing.T) {
	cases := []struct {
		name            string
		saved, reported int
		wantCamera      int
		wantOK          bool
	}{
		{"no saved preference -> no action", NoCameraPreference, 0, -1, false},
		{"saved matches reported -> no action", 1, 1, -1, false},
		{"saved differs from reported -> restore saved", 1, 0, 1, true},
		{"saved differs the other way -> restore saved", 0, 1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCamera, gotOK := decideCameraRestore(tc.saved, tc.reported)
			if gotCamera != tc.wantCamera || gotOK != tc.wantOK {
				t.Errorf("decideCameraRestore(%d, %d) = (%d, %v), want (%d, %v)",
					tc.saved, tc.reported, gotCamera, gotOK, tc.wantCamera, tc.wantOK)
			}
		})
	}
}
