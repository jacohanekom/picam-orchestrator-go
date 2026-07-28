package recorder

import (
	"testing"
	"time"

	"picam-orchestrator/internal/detect"
)

// newTestRecorder returns an EventRecorder backed by a real (but never
// actually exercised by these tests) video Recorder -- every test here
// pokes EventRecorder's own bookkeeping fields directly rather than
// going through vrec.Start(), so its dir/resolution are never touched.
func newTestRecorder(idleSecs int) *EventRecorder {
	vrec := NewRecorder("/tmp/picam-orchestrator-test", 64, 64, 10, 1, 1)
	return New(vrec, idleSecs, nil)
}

func TestStartManualSetsManualActive(t *testing.T) {
	r := newTestRecorder(0)
	r.StartManual()
	_, manual := r.Status()
	if !manual {
		t.Fatalf("StartManual should mark the recorder as manually active")
	}
}

func TestStopManualClearsManualActive(t *testing.T) {
	r := newTestRecorder(0)
	r.StartManual()
	r.StopManual()
	_, manual := r.Status()
	if manual {
		t.Fatalf("StopManual should clear manual mode")
	}
}

func TestStopManualForcesStopEvenWithHaveEvents(t *testing.T) {
	r := newTestRecorder(0)
	r.mu.Lock()
	r.recording = true
	r.haveEvents = true // detections still "want" it recording
	r.mu.Unlock()

	r.StopManual()

	r.mu.Lock()
	stopRequested := r.stopRequested
	r.mu.Unlock()
	if !stopRequested {
		t.Fatalf("StopManual should force a stop even while haveEvents is still true")
	}
}

func TestNotifyEmptyDetectionsDoesNotStopManualRecording(t *testing.T) {
	r := newTestRecorder(0)
	r.mu.Lock()
	r.recording = true
	r.manualActive = true
	r.mu.Unlock()

	r.Notify(detect.Event{}) // empty detections

	r.mu.Lock()
	stopRequested := r.stopRequested
	r.mu.Unlock()
	if stopRequested {
		t.Fatalf("an empty-detections Notify should not request a stop while manually active")
	}
}

func TestNotifyEmptyDetectionsStopsNonManualRecording(t *testing.T) {
	r := newTestRecorder(0)
	r.mu.Lock()
	r.recording = true
	r.mu.Unlock()

	r.Notify(detect.Event{})

	r.mu.Lock()
	stopRequested := r.stopRequested
	r.mu.Unlock()
	if !stopRequested {
		t.Fatalf("an empty-detections Notify should request a stop for a non-manual recording")
	}
}

func TestTickIdleWatchdogSkipsManualRecording(t *testing.T) {
	r := newTestRecorder(1) // 1s idle timeout
	r.mu.Lock()
	r.recording = true
	r.manualActive = true
	r.currentFile = "/tmp/whatever.mp4"
	r.lastDetectionAt = time.Now().Add(-10 * time.Second) // long past the idle timeout
	r.mu.Unlock()

	r.tick()

	r.mu.Lock()
	stillRecording := r.recording
	r.mu.Unlock()
	if !stillRecording {
		t.Fatalf("idle-timeout watchdog should not stop a manually active recording")
	}
}

func TestTickIdleWatchdogStopsNonManualRecording(t *testing.T) {
	r := newTestRecorder(1)
	r.mu.Lock()
	r.recording = true
	r.currentFile = "/tmp/whatever.mp4"
	r.lastDetectionAt = time.Now().Add(-10 * time.Second)
	r.mu.Unlock()

	r.tick()

	r.mu.Lock()
	stillRecording := r.recording
	r.mu.Unlock()
	if stillRecording {
		t.Fatalf("idle-timeout watchdog should stop a non-manual recording")
	}
}

func TestFlushClearsManualActive(t *testing.T) {
	r := newTestRecorder(0)
	r.mu.Lock()
	r.recording = true
	r.manualActive = true
	r.currentFile = "" // empty path -- saveEvents becomes a no-op
	r.mu.Unlock()

	r.flush()

	_, manual := r.Status()
	if manual {
		t.Fatalf("flush should clear manualActive regardless of how the recording ended")
	}
}
