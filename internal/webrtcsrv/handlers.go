package webrtcsrv

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"

	"picam-orchestrator/internal/camrpc"
)

func (s *Server) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("POST /webrtc/offer", s.handleOffer)
	mux.HandleFunc("GET /select", s.handleSelect)
	mux.HandleFunc("GET /osd", s.handleOSD)
	mux.HandleFunc("GET /annotate", s.handleAnnotate)
	mux.HandleFunc("GET /camera", s.handleCamera)
	mux.HandleFunc("GET /lux-switch", s.handleLuxSwitch)
	mux.HandleFunc("GET /ir-light", s.handleIRLight)
	mux.HandleFunc("GET /status.json", s.handleStatusJSON)
	mux.HandleFunc("GET /debug/frame.jpg", s.handleDebugFrame)
	mux.HandleFunc("GET /debug/frame.raw", s.handleDebugFrameRaw)
	mux.HandleFunc("/", handleNotFound)
}

// handleDebugFrame implements GET /debug/frame.jpg?stream=main|lores. It
// JPEG-encodes the current live frame directly from the mailbox,
// bypassing VP8/WebRTC, so a headless box can `curl` it to see whether
// the frame feeding the encoder is already corrupt.
func (s *Server) handleDebugFrame(w http.ResponseWriter, r *http.Request) {
	if s.cfg.DebugFrameJPEG == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "debug frame endpoint disabled"})
		return
	}
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMainHigh)
	jpg, ok := s.cfg.DebugFrameJPEG(stream)
	if !ok || len(jpg) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no frame available yet"})
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jpg)
}

// handleDebugFrameRaw implements GET /debug/frame.raw?stream=main|lores.
// It returns the current mailbox frame's raw I420 bytes with no re-encode,
// plus diagnostic headers reporting the frame's width/height, actual data
// length, and the length expected for those dimensions. A mismatch
// between actual and expected length (visible with `curl -sI`) points
// straight at a reassembly/size bug; matching lengths with corrupt bytes
// points at a packing/plane bug to be found in the dumped bytes.
func (s *Server) handleDebugFrameRaw(w http.ResponseWriter, r *http.Request) {
	if s.cfg.DebugFrameRaw == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "debug raw endpoint disabled"})
		return
	}
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMainHigh)
	data, fw, fh, ok := s.cfg.DebugFrameRaw(stream)
	if !ok || len(data) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no frame available yet"})
		return
	}
	expected := fw * fh * 3 / 2
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Frame-Width", strconv.Itoa(fw))
	w.Header().Set("X-Frame-Height", strconv.Itoa(fh))
	w.Header().Set("X-Frame-Datalen", strconv.Itoa(len(data)))
	w.Header().Set("X-Frame-Expectedlen", strconv.Itoa(expected))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		h.ServeHTTP(w, r)
	})
}

func handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte("Not found"))
}

// parseBoolParam parses a query-param value that's true only for the
// exact (case-sensitive) strings "true", "1", or "yes". An empty/absent
// value means "leave the current setting unchanged" (present=false) —
// this is how a caller reads current state without changing it, by
// passing no params at all.
func parseBoolParam(v string) (val bool, present bool) {
	if v == "" {
		return false, false
	}
	return v == "true" || v == "1" || v == "yes", true
}

// handleSelect implements GET /select?stream=<name>. It validates and
// echoes the stream name for client/UI sync; real per-client stream
// selection happens via the offer's own ?stream= param.
func (s *Server) handleSelect(w http.ResponseWriter, r *http.Request) {
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMainHigh)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stream": stream.String()})
}

// handleOSD implements GET /osd?camera_id=<bool>&time=<bool>&enabled=<bool>.
// A runtime change here is persisted (see internal/uistate) so it
// survives a restart, unlike this handler's own atomic fields.
func (s *Server) handleOSD(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var cameraIDPtr, timePtr *bool
	if v, ok := parseBoolParam(q.Get("enabled")); ok {
		cameraIDPtr, timePtr = &v, &v
	}
	if v, ok := parseBoolParam(q.Get("camera_id")); ok {
		cameraIDPtr = &v
	}
	if v, ok := parseBoolParam(q.Get("time")); ok {
		timePtr = &v
	}
	if cameraIDPtr != nil {
		s.OSDCameraID.Store(*cameraIDPtr)
	}
	if timePtr != nil {
		s.OSDTime.Store(*timePtr)
	}
	s.uiState.SetOSD(cameraIDPtr, timePtr)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                true,
		"camera_id_enabled": s.OSDCameraID.Load(),
		"time_enabled":      s.OSDTime.Load(),
	})
}

// handleAnnotate implements GET /annotate?lores=<bool>&main=<bool>. A
// runtime change here is persisted (see internal/uistate) so it
// survives a restart.
func (s *Server) handleAnnotate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var mainPtr, loresPtr *bool
	if v, ok := parseBoolParam(q.Get("lores")); ok {
		loresPtr = &v
	}
	if v, ok := parseBoolParam(q.Get("main")); ok {
		mainPtr = &v
	}
	if loresPtr != nil {
		s.LoresAnnotated.Store(*loresPtr)
	}
	if mainPtr != nil {
		s.MainAnnotated.Store(*mainPtr)
	}
	s.uiState.SetAnnotate(mainPtr, loresPtr)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"lores_annotated": s.LoresAnnotated.Load(),
		"main_annotated":  s.MainAnnotated.Load(),
	})
}

// handleCamera implements GET /camera?id=<N>, proxying picam-raw's raw
// JSON response through verbatim. A successful switch is persisted
// (see internal/uistate) so it's restored after a restart if picam-raw
// itself reports something different by then.
func (s *Server) handleCamera(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" || strings.ContainsFunc(idStr, func(c rune) bool { return c < '0' || c > '9' }) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	id, _ := strconv.Atoi(idStr)

	reached, resp := camrpc.SwitchCamera(s.cfg.PicamRawHost, s.cfg.PicamRawCmdPort, id)
	if reached {
		s.uiState.SetActiveCamera(id)
	}
	status := http.StatusOK
	if !reached {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(resp))
}

// handleLuxSwitch implements GET /lux-switch?enabled=<bool>&threshold=<int>.
// The actual lux-crossing evaluation and camera switch runs in
// internal/luxswitch's own background loop, not here -- this handler
// only reads/updates its live configuration (persisted to disk by
// luxswitch.State.Set, so it survives a restart).
func (s *Server) handleLuxSwitch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var enabledPtr *bool
	if v, ok := parseBoolParam(q.Get("enabled")); ok {
		enabledPtr = &v
	}

	var thresholdPtr *int
	if raw := q.Get("threshold"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid threshold"})
			return
		}
		thresholdPtr = &v
	}

	enabled, threshold := s.luxSwitch.Set(enabledPtr, thresholdPtr)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                   true,
		"lux_switch_enabled":   enabled,
		"lux_switch_threshold": threshold,
	})
}

// handleIRLight implements GET /ir-light?enabled=<bool>&threshold=<int>&max_on_minutes=<int>.
// The actual lux-crossing evaluation and relay command runs in
// internal/irlight's own background loop, not here -- this handler
// only reads/updates its live configuration (persisted to disk by
// irlight.State.Set, so it survives a restart).
func (s *Server) handleIRLight(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var enabledPtr *bool
	if v, ok := parseBoolParam(q.Get("enabled")); ok {
		enabledPtr = &v
	}

	var thresholdPtr *int
	if raw := q.Get("threshold"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid threshold"})
			return
		}
		thresholdPtr = &v
	}

	var maxOnMinutesPtr *int
	if raw := q.Get("max_on_minutes"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid max_on_minutes"})
			return
		}
		maxOnMinutesPtr = &v
	}

	enabled, threshold, maxOnMinutes := s.irLight.Set(enabledPtr, thresholdPtr, maxOnMinutesPtr)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                      true,
		"ir_light_enabled":        enabled,
		"ir_light_threshold":      threshold,
		"ir_light_max_on_minutes": maxOnMinutes,
	})
}

func round1(v float32) float64 {
	return math.Round(float64(v)*10) / 10
}

// handleStatusJSON implements GET /status.json.
func (s *Server) handleStatusJSON(w http.ResponseWriter, r *http.Request) {
	total, mainHigh, mainLow, loresClients := s.ClientCounts()
	snap := s.status.Snapshot()
	tel := s.telemetry.Snapshot()
	luxEnabled, luxThreshold := s.luxSwitch.Get()
	irEnabled, irThreshold, irMaxOnMinutes := s.irLight.Get()

	writeJSON(w, http.StatusOK, map[string]any{
		"clients":                 total,
		"camera_id_enabled":       s.OSDCameraID.Load(),
		"time_enabled":            s.OSDTime.Load(),
		"lores_annotated":         s.LoresAnnotated.Load(),
		"main_annotated":          s.MainAnnotated.Load(),
		"lux_switch_enabled":      luxEnabled,
		"lux_switch_threshold":    luxThreshold,
		"ir_light_enabled":        irEnabled,
		"ir_light_threshold":      irThreshold,
		"ir_light_max_on_minutes": irMaxOnMinutes,
		"frame_ts_us":             snap.LatestFrameTsUs,
		"streams": map[string]int{
			"main":      mainHigh + mainLow,
			"main_high": mainHigh,
			"main_low":  mainLow,
			"lores":     loresClients,
		},
		"telemetry": map[string]any{
			"connected":     tel.Connected,
			"lux":           round1(tel.Lux),
			"active_camera": tel.ActiveCamera,
			"camera_label":  tel.CameraLabel,
		},
	})
}
