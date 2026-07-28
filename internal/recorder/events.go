package recorder

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Recording describes one completed (or in-progress) recording found on
// disk in picam-recorder's output directory.
type Recording struct {
	Name      string    `json:"name"`       // filename stem, e.g. a UUID or "manual-<uuid>" -- see EventRecorder
	StartTime time.Time `json:"start_time"` // best-effort recording start; see ListRecordings
	SizeBytes int64     `json:"size_bytes"`
}

// nameRE matches exactly the filename stems EventRecorder ever
// generates (a UUID, optionally "manual-"-prefixed) -- used both to
// filter ListRecordings' directory scan and, more importantly, to
// validate the ?name= query param on the download endpoint: since it's
// an allowlist of the only characters ever legitimately present, it
// rejects any path-traversal attempt (no "/", "\", or "..") by
// construction rather than by trying to blocklist those specifically.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// ValidRecordingName reports whether name is safe to join onto
// RecorderDir and use as a file path -- see nameRE.
func ValidRecordingName(name string) bool {
	return name != "" && nameRE.MatchString(name)
}

// ListRecordings scans dir for *.avi files and returns one Recording
// per file, newest first. Each recording's StartTime comes from its
// .csv sidecar's first data row (the true wall-clock start, including
// any flushed pre-buffer frames written ahead of the trigger) when
// that file is present and parseable, falling back to the .avi's own
// mtime otherwise (e.g. an in-progress recording, whose .csv is only
// written on close).
func ListRecordings(dir string) ([]Recording, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Recording, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".avi") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".avi")
		if !ValidRecordingName(name) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		start := info.ModTime()
		if t, ok := firstCSVTimestamp(filepath.Join(dir, name+".csv")); ok {
			start = t
		}
		out = append(out, Recording{
			Name:      name,
			StartTime: start,
			SizeBytes: info.Size(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartTime.After(out[j].StartTime) })
	return out, nil
}

// firstCSVTimestamp reads just the first data row of a picam-recorder
// .csv sidecar (frame,frame_seq,ts_us,rtp_time,wall_time,nal_type) and
// parses its wall_time (RFC3339) column.
func firstCSVTimestamp(path string) (time.Time, bool) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil { // header row
		return time.Time{}, false
	}
	row, err := r.Read()
	if err != nil || len(row) < 5 {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, row[4])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
