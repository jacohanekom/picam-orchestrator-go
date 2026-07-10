package recorder

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"picam-orchestrator/internal/detect"
)

// SnapshotFunc returns JPEG bytes to save alongside a newly started
// recording (annotated with the triggering event's detection boxes), or
// nil to skip the snapshot.
type SnapshotFunc func(detect.Event) []byte

// EventRecorder starts/stops picam-recorder in response to detection
// activity and writes a JSON event log alongside each recording.
type EventRecorder struct {
	host       string
	port       int
	snapshotFn SnapshotFunc

	mu            sync.Mutex
	haveEvents    bool
	accumulated   []detect.Event
	recording     bool
	stopRequested bool
	currentFile   string
	startedUs     int64

	wake chan struct{}
}

// New creates an EventRecorder targeting picam-recorder at host:port.
// snapshotFn may be nil to disable snapshot files.
func New(host string, port int, snapshotFn SnapshotFunc) *EventRecorder {
	return &EventRecorder{
		host:       host,
		port:       port,
		snapshotFn: snapshotFn,
		wake:       make(chan struct{}, 1),
	}
}

// Notify reports a detection event from picam-hailo. A non-empty
// detection list marks the recorder as having something to record
// (accumulated for the eventual .events.json, but not itself starting
// the recording — that happens in Run's loop); an empty list requests
// an immediate stop if currently recording.
func (r *EventRecorder) Notify(evt detect.Event) {
	r.mu.Lock()
	if len(evt.Detections) == 0 {
		if r.recording {
			r.stopRequested = true
		}
	} else {
		r.haveEvents = true
		r.accumulated = append(r.accumulated, evt)
	}
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run drives the recorder's worker loop until ctx is cancelled, flushing
// any in-progress recording before returning.
func (r *EventRecorder) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			recording := r.recording
			r.mu.Unlock()
			if recording {
				r.flush()
			}
			return
		case <-r.wake:
		case <-time.After(time.Second):
		}
		r.tick()
	}
}

func (r *EventRecorder) tick() {
	r.mu.Lock()
	startRecording := r.haveEvents && !r.recording
	var triggerEvt detect.Event
	if startRecording && len(r.accumulated) > 0 {
		triggerEvt = r.accumulated[0]
	}
	r.mu.Unlock()

	if startRecording {
		id := uuid.New().String()
		startedUs := time.Now().UnixMicro()
		file := Start(r.host, r.port, id)

		if file != "" && r.snapshotFn != nil && len(triggerEvt.Detections) > 0 {
			if jpg := r.snapshotFn(triggerEvt); len(jpg) > 0 {
				imgPath := strings.TrimSuffix(file, filepath.Ext(file)) + ".jpg"
				if err := os.WriteFile(imgPath, jpg, 0o644); err != nil {
					log.Printf("[EventRecorder] Cannot write snapshot %s: %v", imgPath, err)
				} else {
					log.Printf("[EventRecorder] Snapshot: %s", imgPath)
				}
			}
		}

		r.mu.Lock()
		if file != "" {
			r.recording = true
			r.currentFile = file
			r.startedUs = startedUs
			log.Printf("[EventRecorder] Started: %s", file)
		} else {
			log.Printf("[EventRecorder] Failed to start recorder")
			r.accumulated = nil
			r.haveEvents = false
		}
		r.mu.Unlock()
	}

	r.mu.Lock()
	stopNow := r.recording && r.stopRequested
	if stopNow {
		r.stopRequested = false
	}
	r.mu.Unlock()

	if stopNow {
		r.flush()
	}
}

// flush stops the current recording and writes its accumulated events.
func (r *EventRecorder) flush() {
	r.mu.Lock()
	file := r.currentFile
	events := r.accumulated
	startedUs := r.startedUs
	r.recording = false
	r.currentFile = ""
	r.haveEvents = false
	r.accumulated = nil
	r.mu.Unlock()

	Stop(r.host, r.port)
	saveEvents(file, events, startedUs)
}

type eventsFile struct {
	Recording string        `json:"recording"`
	StartedUs int64         `json:"started_us"`
	Events    []eventRecord `json:"events"`
}

type eventRecord struct {
	TsUs       int64    `json:"ts_us"`
	FrameSeq   uint32   `json:"frame_seq"`
	Detections []detRec `json:"detections"`
}

type detRec struct {
	Class string  `json:"class"`
	Conf  float32 `json:"conf"`
	X0    float32 `json:"x0"`
	Y0    float32 `json:"y0"`
	X1    float32 `json:"x1"`
	Y1    float32 `json:"y1"`
}

// saveEvents writes (or appends to, via full read-modify-write) the
// <recording-without-ext>.events.json file. Unlike the C++ original's
// byte-splicing trick, this always parses+re-marshals the whole file;
// the resulting parsed JSON shape is identical, which is the only
// requirement consumers have.
func saveEvents(mp4path string, events []detect.Event, startedUs int64) {
	if mp4path == "" {
		return
	}
	jsonPath := strings.TrimSuffix(mp4path, filepath.Ext(mp4path)) + ".events.json"

	ef := eventsFile{Recording: mp4path, StartedUs: startedUs}
	if data, err := os.ReadFile(jsonPath); err == nil {
		if uerr := json.Unmarshal(data, &ef); uerr != nil {
			log.Printf("[EventRecorder] Cannot parse existing %s, overwriting: %v", jsonPath, uerr)
			ef = eventsFile{Recording: mp4path, StartedUs: startedUs}
		}
	}

	for _, e := range events {
		rec := eventRecord{TsUs: e.TsUs, FrameSeq: e.FrameSeq}
		for _, d := range e.Detections {
			rec.Detections = append(rec.Detections, detRec{
				Class: d.Class, Conf: d.Conf,
				X0: d.X0, Y0: d.Y0, X1: d.X1, Y1: d.Y1,
			})
		}
		ef.Events = append(ef.Events, rec)
	}

	out, err := json.MarshalIndent(ef, "", "  ")
	if err != nil {
		log.Printf("[EventRecorder] marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(jsonPath, out, 0o644); err != nil {
		log.Printf("[EventRecorder] Cannot write %s: %v", jsonPath, err)
		return
	}
	log.Printf("[EventRecorder] Wrote %s (%d new events)", jsonPath, len(events))
}
