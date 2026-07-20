// Package config loads picam-orchestrator's INI-style configuration file.
//
// The file format matches the original C++ implementation's hand-rolled
// parser exactly: "[section]" headers, "key = value" pairs (value is
// everything after the first '=' up to the first unquoted ';' or '#',
// trimmed), blank lines and full-line comments ignored, keys stored flat
// as "section.key" (or bare "key" if no section has been seen yet).
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// rawStore is the flat "section.key" -> value map produced by parsing,
// kept around only long enough to populate the typed Config below.
type rawStore map[string]string

func parseFile(path string) (rawStore, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open config: %w", err)
	}
	defer f.Close()

	store := rawStore{}
	section := ""
	scanner := bufio.NewScanner(f)
	lineno := 0
	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == ';' || line[0] == '#' {
			continue
		}

		if line[0] == '[' {
			end := strings.IndexByte(line, ']')
			if end < 0 {
				return nil, fmt.Errorf("%s:%d: unclosed '['", path, lineno)
			}
			section = strings.TrimSpace(line[1:end])
			continue
		}

		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(stripComment(line[eq+1:]))
		if key == "" {
			continue
		}
		if section != "" {
			key = section + "." + key
		}
		store[key] = val
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return store, nil
}

// stripComment removes a trailing ';' or '#' comment, ignoring either
// character while inside a double-quoted span (INI values here are
// normally unquoted, so this only matters for the rare quoted value).
func stripComment(s string) string {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case ';', '#':
			if !inQuote {
				return s[:i]
			}
		}
	}
	return s
}

// sharedStreamConfigPath is the aipicam-config package's shared source
// of truth for picam-raw's stream geometry/ports. picam-raw's wire
// protocol carries no width/height field, so every reader must already
// agree with what picam-raw actually sends — confirmed missing before:
// this package shipped 1920x1080 for main while picam-raw sends
// 2304x1296, causing frame corruption with no error raised anywhere.
const sharedStreamConfigPath = "/etc/aipicam/streams.conf"

// applySharedStreamDefaults fills r with values from the shared stream
// config for any of the given keys r doesn't already set explicitly —
// so this package's own config.ini can still override for local
// debugging, but a stock install (no explicit input.*/telemetry.* keys)
// always reflects the shared file instead of duplicating it. Silently
// does nothing if the shared file isn't present (aipicam-config not
// installed), falling back to this package's own literal defaults.
func applySharedStreamDefaults(r rawStore) {
	shared, err := parseFile(sharedStreamConfigPath)
	if err != nil {
		return
	}
	apply := func(dstKey, srcKey string) {
		if _, ok := r[dstKey]; ok {
			return
		}
		if v, ok := shared[srcKey]; ok {
			r[dstKey] = v
		}
	}
	apply("input.main_width", "stream.main_width")
	apply("input.main_height", "stream.main_height")
	apply("input.lores_width", "stream.lores_width")
	apply("input.lores_height", "stream.lores_height")
	apply("input.main_port", "stream.main_port")
	apply("input.lores_port", "stream.lores_port")
	apply("telemetry.port", "stream.telemetry_port")
	apply("telemetry.command_port", "stream.command_port")
}

func (r rawStore) str(key, def string) string {
	if v, ok := r[key]; ok {
		return v
	}
	return def
}

func (r rawStore) int(key string, def int) int {
	v, ok := r[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func (r rawStore) boolean(key string, def bool) bool {
	v, ok := r[key]
	if !ok || v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "true", "1", "yes":
		return true
	}
	return false
}

// Config holds every setting picam-orchestrator needs, parsed once at
// startup into typed fields (unlike the C++ original's runtime
// get_str/get_int/... lookups against a flat map).
type Config struct {
	// [input]
	InputHost   string
	MainPort    int
	MainWidth   int
	MainHeight  int
	LoresPort   int
	LoresWidth  int
	LoresHeight int
	PingEvery   int

	// [detections]
	DetectionsHost string
	DetectionsPort int
	ToleranceMs    int

	// [telemetry]
	TelemetryHost string
	TelemetryPort int
	CommandPort   int

	// [delay]
	DelayMs int

	// [encode]
	VP8BitrateMainKbps  int
	VP8BitrateLoresKbps int
	VP8CPUUsedMain      int
	VP8CPUUsedLores     int
	JPEGQuality         int
	OutputFPSLive       int
	OutputFPSAnnotated  int

	// [webrtc]
	ICEPortMin int
	ICEPortMax int

	// [annotate]
	AnnotateLores bool
	AnnotateMain  bool

	// [osd]
	OSDCameraID     bool
	OSDTime         bool
	OSDCameraLabels []string

	// [output]
	HTTPPort      int
	StatusPort    int
	DefaultStream string

	// [recorder]
	RecorderHost string
	RecorderPort int
	// RecorderIdleSecs is parsed for config-file compatibility but, matching
	// the C++ original, is never actually consulted: EventRecorder stops
	// recording immediately on the first empty-detections event rather than
	// after a timed idle period.
	RecorderIdleSecs int
}

// Load reads and parses path, applying the same defaults the C++
// implementation falls back to when a key is entirely absent.
func Load(path string) (*Config, error) {
	r, err := parseFile(path)
	if err != nil {
		return nil, err
	}
	applySharedStreamDefaults(r)

	labelsRaw := r.str("osd.camera_labels", "")
	var labels []string
	if labelsRaw != "" {
		for _, l := range strings.Split(labelsRaw, ",") {
			labels = append(labels, strings.TrimSpace(l))
		}
	}

	c := &Config{
		InputHost:   r.str("input.host", "127.0.0.1"),
		MainPort:    r.int("input.main_port", 8560),
		MainWidth:   r.int("input.main_width", 2304),
		MainHeight:  r.int("input.main_height", 1296),
		LoresPort:   r.int("input.lores_port", 8561),
		LoresWidth:  r.int("input.lores_width", 640),
		LoresHeight: r.int("input.lores_height", 360),
		PingEvery:   r.int("input.ping_every", 5),

		DetectionsHost: r.str("detections.host", "127.0.0.1"),
		DetectionsPort: r.int("detections.port", 8558),
		ToleranceMs:    r.int("detections.tolerance_ms", 150),

		TelemetryHost: r.str("telemetry.host", "127.0.0.1"),
		TelemetryPort: r.int("telemetry.port", 8555),
		CommandPort:   r.int("telemetry.command_port", 8556),

		DelayMs: r.int("delay.delay_ms", 1000),

		VP8BitrateMainKbps:  r.int("encode.vp8_bitrate_main_kbps", 2000),
		VP8BitrateLoresKbps: r.int("encode.vp8_bitrate_lores_kbps", 500),
		// VP8 realtime speed (VP8E_SET_CPUUSED): higher = faster encode,
		// lower quality. Main (full-res, ~13x lores's pixels) defaults
		// faster so its encode keeps up with real time on the Pi; lores
		// has ample headroom and stays at the original 8. Valid range for
		// VP8 realtime is roughly 4-16.
		VP8CPUUsedMain:  r.int("encode.vp8_cpu_used_main", 12),
		VP8CPUUsedLores: r.int("encode.vp8_cpu_used_lores", 8),
		JPEGQuality:     r.int("encode.jpeg_quality", 80),
		OutputFPSLive:       r.int("encode.output_fps_live", 15),
		OutputFPSAnnotated:  r.int("encode.output_fps_annotated", 30),

		ICEPortMin: r.int("webrtc.ice_port_min", 50000),
		ICEPortMax: r.int("webrtc.ice_port_max", 50100),

		AnnotateLores: r.boolean("annotate.lores", false),
		AnnotateMain:  r.boolean("annotate.main", false),

		OSDCameraID:     r.boolean("osd.camera_id", false),
		OSDTime:         r.boolean("osd.time", false),
		OSDCameraLabels: labels,

		HTTPPort:      r.int("output.http_port", 81),
		StatusPort:    r.int("output.status_port", 8091),
		DefaultStream: r.str("output.default_stream", "main"),

		RecorderHost:     r.str("recorder.host", "127.0.0.1"),
		RecorderPort:     r.int("recorder.port", 8080),
		RecorderIdleSecs: r.int("recorder.idle_secs", 30),
	}
	return c, nil
}

// CameraLabel returns the configured display label for cameraIndex, or
// its bare numeric string if no label was configured for that slot.
func (c *Config) CameraLabel(cameraIndex int) string {
	if cameraIndex >= 0 && cameraIndex < len(c.OSDCameraLabels) && c.OSDCameraLabels[cameraIndex] != "" {
		return c.OSDCameraLabels[cameraIndex]
	}
	return strconv.Itoa(cameraIndex)
}
