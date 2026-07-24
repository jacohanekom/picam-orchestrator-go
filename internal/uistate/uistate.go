// Package uistate persists the Settings-page controls picam-frontend
// exposes — OSD overlay (camera ID/time), annotation (main/lores), and
// the last-selected camera lens — to disk, so they survive a
// picam-orchestrator restart. Mirrors internal/luxswitch's own
// enabled/threshold persistence exactly.
//
// Unlike luxswitch (evaluated once every 5s), OSD/annotate are read on
// every single encode tick via webrtcsrv.Server's own atomic.Bool
// fields — this package is never that hot-path source of truth, only a
// write-through side channel: handlers persist here in addition to
// storing on the atomic fields, and main.go reads a Snapshot once at
// startup to seed them.
package uistate

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

// NoCameraPreference means no camera has ever been successfully
// selected via /camera yet — ReconcileActiveCamera leaves picam-raw's
// own current camera alone in this case, rather than forcing camera 0.
const NoCameraPreference = -1

type persisted struct {
	OSDCameraID   bool `json:"osd_camera_id"`
	OSDTime       bool `json:"osd_time"`
	AnnotateMain  bool `json:"annotate_main"`
	AnnotateLores bool `json:"annotate_lores"`
	ActiveCamera  int  `json:"active_camera"`
}

// State holds the persisted settings, guarded by a mutex since they're
// written from HTTP handlers (and read at startup) concurrently.
type State struct {
	mu        sync.Mutex
	statePath string
	data      persisted
}

// New builds a State seeded with the given defaults (normally read from
// config.ini), then overrides them from a previously persisted state
// file at statePath if one exists. statePath may be empty, in which
// case persistence is silently disabled (defaults are used, Set never
// writes to disk) — useful for tests.
func New(statePath string, defaultOSDCameraID, defaultOSDTime, defaultAnnotateMain, defaultAnnotateLores bool) *State {
	s := &State{
		statePath: statePath,
		data: persisted{
			OSDCameraID:   defaultOSDCameraID,
			OSDTime:       defaultOSDTime,
			AnnotateMain:  defaultAnnotateMain,
			AnnotateLores: defaultAnnotateLores,
			ActiveCamera:  NoCameraPreference,
		},
	}
	s.load()
	return s
}

// load overwrites the seeded defaults from disk. A missing or corrupt
// file is not an error — it just means no persisted value exists yet,
// so the caller's defaults stand, same tolerance as luxswitch.State.load.
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
		log.Printf("[UIState] ignoring corrupt state file %s: %v", s.statePath, err)
		return
	}
	s.data = p
}

// save writes the current settings to disk. Persistence is a
// nice-to-have, not a boot-critical dependency, so a failure here is
// logged and swallowed rather than propagated.
func (s *State) save() {
	if s.statePath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		log.Printf("[UIState] failed to create state dir for %s: %v", s.statePath, err)
		return
	}
	data, err := json.Marshal(s.data)
	if err != nil {
		return
	}
	if err := os.WriteFile(s.statePath, data, 0o644); err != nil {
		log.Printf("[UIState] failed to persist state to %s: %v", s.statePath, err)
	}
}

// Snapshot returns every persisted field, for startup use.
func (s *State) Snapshot() (osdCameraID, osdTime, annotateMain, annotateLores bool, activeCamera int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data
	return d.OSDCameraID, d.OSDTime, d.AnnotateMain, d.AnnotateLores, d.ActiveCamera
}

// SetOSD updates whichever of cameraID/timeVal is non-nil, leaving the
// other unchanged (same "absent param = no change" convention as the
// /osd handler itself), and persists the result.
func (s *State) SetOSD(cameraID, timeVal *bool) {
	s.mu.Lock()
	if cameraID != nil {
		s.data.OSDCameraID = *cameraID
	}
	if timeVal != nil {
		s.data.OSDTime = *timeVal
	}
	s.mu.Unlock()
	s.save()
}

// SetAnnotate updates whichever of main/lores is non-nil, leaving the
// other unchanged, and persists the result.
func (s *State) SetAnnotate(main, lores *bool) {
	s.mu.Lock()
	if main != nil {
		s.data.AnnotateMain = *main
	}
	if lores != nil {
		s.data.AnnotateLores = *lores
	}
	s.mu.Unlock()
	s.save()
}

// SetActiveCamera records camera as the last-selected lens and persists
// it, so ReconcileActiveCamera can restore it after a restart.
func (s *State) SetActiveCamera(camera int) {
	s.mu.Lock()
	s.data.ActiveCamera = camera
	s.mu.Unlock()
	s.save()
}

// decideCameraRestore returns the camera to switch to (and true) if a
// saved preference exists and differs from what picam-raw currently
// reports, or (-1, false) if no action is needed. Pure and side-effect
// free so it's cheap to unit test independently of telemetry/network
// state, mirroring luxswitch.decideSwitch.
func decideCameraRestore(savedPreference, reportedCamera int) (want int, ok bool) {
	if savedPreference == NoCameraPreference || savedPreference == reportedCamera {
		return -1, false
	}
	return savedPreference, true
}

// ReconcileActiveCamera waits (up to timeout) for picam-raw's telemetry
// to connect, then — if a saved camera preference exists and differs
// from whatever picam-raw currently reports — issues a one-shot
// camera-switch RPC to restore it. A no-op in the common case where
// picam-raw already agrees (its own hardware state doesn't change just
// because this process restarted). Runs once and returns; not a
// long-lived loop like luxswitch.Run.
func ReconcileActiveCamera(ctx context.Context, state *State, tel *telemetry.State, host string, cmdPort int, timeout time.Duration) {
	_, _, _, _, want := state.Snapshot()
	if want == NoCameraPreference {
		return
	}

	deadline := time.Now().Add(timeout)
	for !tel.Snapshot().Connected {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			log.Printf("[UIState] telemetry never connected within %v — skipping camera restore", timeout)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}

	got := tel.Snapshot().ActiveCamera
	target, ok := decideCameraRestore(want, got)
	if !ok {
		return
	}
	if reached, _ := camrpc.SwitchCamera(host, cmdPort, target); reached {
		log.Printf("[UIState] restored camera %d after restart (picam-raw reported %d)", target, got)
	} else {
		log.Printf("[UIState] failed to restore camera %d after restart", target)
	}
}
