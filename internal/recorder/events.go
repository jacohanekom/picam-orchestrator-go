package recorder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Recording describes one completed (or in-progress) recording found on
// disk in RecorderDir: a .webm file written by this process's own
// Recorder, or a legacy .mp4 left over from picam-recorder's era.
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

// RecordingExts are tried in order by both ListRecordings and the
// download handler: .webm for everything this process records itself,
// .mp4 for anything left over from picam-recorder's era.
var RecordingExts = []string{".webm", ".mp4"}

// ListRecordings scans dir for *.webm and *.mp4 files and returns one
// Recording per file, newest first. Each recording's StartTime comes
// from its <name>.events.json sidecar's started_us field (written by
// EventRecorder for every recording, automatic or manual) when present
// and parseable, falling back to the file's own mtime otherwise (e.g.
// an in-progress recording, whose sidecar is only written on stop).
func ListRecordings(dir string) ([]Recording, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Recording, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var name string
		for _, ext := range RecordingExts {
			if strings.HasSuffix(e.Name(), ext) {
				name = strings.TrimSuffix(e.Name(), ext)
				break
			}
		}
		if name == "" || !ValidRecordingName(name) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		start := info.ModTime()
		if t, ok := eventsJSONStartTime(filepath.Join(dir, name+".events.json")); ok {
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

// eventsJSONStartTime reads a recording's .events.json sidecar (see
// eventrecorder.go's saveEvents) and returns its started_us field as a
// time.Time.
func eventsJSONStartTime(path string) (time.Time, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, false
	}
	var ef eventsFile
	if err := json.Unmarshal(data, &ef); err != nil || ef.StartedUs == 0 {
		return time.Time{}, false
	}
	return time.UnixMicro(ef.StartedUs), true
}
