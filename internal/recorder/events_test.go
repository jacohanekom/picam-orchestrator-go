package recorder

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidRecordingName(t *testing.T) {
	valid := []string{
		"a1b2c3d4-e5f6-4789-9abc-def012345678",
		"manual-a1b2c3d4-e5f6-4789-9abc-def012345678",
		"clip01",
	}
	for _, n := range valid {
		if !ValidRecordingName(n) {
			t.Errorf("ValidRecordingName(%q) = false, want true", n)
		}
	}

	invalid := []string{
		"",
		"..",
		"../../etc/passwd",
		"foo/bar",
		"foo\\bar",
		"foo.mp4",
		"foo bar",
	}
	for _, n := range invalid {
		if ValidRecordingName(n) {
			t.Errorf("ValidRecordingName(%q) = true, want false", n)
		}
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func TestListRecordingsPrefersCSVTimestampOverMtime(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "clip01.mp4"), "fake mp4 bytes")
	writeFile(t, filepath.Join(dir, "clip01.csv"),
		"frame,frame_seq,ts_us,rtp_time,wall_time,nal_type\n"+
			"1,100,0,0,2020-01-01T00:00:00Z,5\n"+
			"2,101,33000,2970,2020-01-01T00:00:00.033Z,1\n")

	recs, err := ListRecordings(dir)
	if err != nil {
		t.Fatalf("ListRecordings: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs) = %d, want 1", len(recs))
	}
	want, _ := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	if !recs[0].StartTime.Equal(want) {
		t.Fatalf("StartTime = %v, want %v (the CSV's first row, not the file's mtime)", recs[0].StartTime, want)
	}
	if recs[0].Name != "clip01" {
		t.Fatalf("Name = %q, want clip01", recs[0].Name)
	}
}

func TestListRecordingsFallsBackToMtimeWithoutCSV(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "clip02.mp4"), "fake mp4 bytes")

	recs, err := ListRecordings(dir)
	if err != nil {
		t.Fatalf("ListRecordings: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs) = %d, want 1", len(recs))
	}
	if recs[0].StartTime.IsZero() {
		t.Fatalf("StartTime is zero, want the file's mtime")
	}
}

func TestListRecordingsIgnoresNonMP4AndInvalidNames(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "clip03.mp4"), "fake mp4 bytes")
	writeFile(t, filepath.Join(dir, "clip03.csv"), "frame,frame_seq,ts_us,rtp_time,wall_time,nal_type\n")
	writeFile(t, filepath.Join(dir, "not-a-recording.txt"), "irrelevant")

	recs, err := ListRecordings(dir)
	if err != nil {
		t.Fatalf("ListRecordings: %v", err)
	}
	if len(recs) != 1 || recs[0].Name != "clip03" {
		t.Fatalf("recs = %+v, want exactly one recording named clip03", recs)
	}
}

func TestListRecordingsSortedNewestFirst(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "older.mp4"), "x")
	writeFile(t, filepath.Join(dir, "older.csv"),
		"frame,frame_seq,ts_us,rtp_time,wall_time,nal_type\n1,1,0,0,2020-01-01T00:00:00Z,5\n")
	writeFile(t, filepath.Join(dir, "newer.mp4"), "x")
	writeFile(t, filepath.Join(dir, "newer.csv"),
		"frame,frame_seq,ts_us,rtp_time,wall_time,nal_type\n1,1,0,0,2021-01-01T00:00:00Z,5\n")

	recs, err := ListRecordings(dir)
	if err != nil {
		t.Fatalf("ListRecordings: %v", err)
	}
	if len(recs) != 2 || recs[0].Name != "newer" || recs[1].Name != "older" {
		t.Fatalf("recs = %+v, want [newer, older]", recs)
	}
}
