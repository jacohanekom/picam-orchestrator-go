package mjpegsrv

import (
	"encoding/json"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"picam-orchestrator/internal/camrpc"
)

func (s *Server) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /stream/main.mjpeg", s.handleStream(StreamMain))
	mux.HandleFunc("GET /stream/lores.mjpeg", s.handleStream(StreamLores))
	mux.HandleFunc("GET /select", s.handleSelect)
	mux.HandleFunc("GET /osd", s.handleOSD)
	mux.HandleFunc("GET /annotate", s.handleAnnotate)
	mux.HandleFunc("GET /camera", s.handleCamera)
	mux.HandleFunc("GET /status.json", s.handleStatusJSON)
	mux.HandleFunc("GET /debug/frame.jpg", s.handleDebugFrame)
	mux.HandleFunc("GET /debug/frame.raw", s.handleDebugFrameRaw)
	mux.HandleFunc("/", handleNotFound)
}

// handleStream implements GET /stream/main.mjpeg and GET
// /stream/lores.mjpeg: a classic multipart/x-mixed-replace MJPEG
// stream, directly usable as an <img src="..."> in a browser (or
// relayed through picam-frontend to one), that runs until the client
// disconnects or the server shuts down. No SDP negotiation, no ICE — a
// single plain HTTP GET.
func (s *Server) handleStream(stream StreamSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if total, _, _ := s.ClientCounts(); total >= s.cfg.MaxClients {
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		mw := multipart.NewWriter(w)
		w.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary="+mw.Boundary())
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		c := newClient(stream)
		s.registerClient(c)
		log.Printf("[MJPEG] client connected, stream=%s", stream)
		defer func() {
			c.markDead()
			log.Printf("[MJPEG] client disconnected, stream=%s", stream)
		}()

		reqCtx := r.Context()
		for {
			select {
			case <-reqCtx.Done(): // client disconnected
				return
			case <-c.done: // server-initiated (e.g. shutdown)
				return
			case jpg := <-c.sendCh:
				pw, err := mw.CreatePart(textproto.MIMEHeader{
					"Content-Type":   {"image/jpeg"},
					"Content-Length": {strconv.Itoa(len(jpg))},
				})
				if err != nil {
					return
				}
				if _, err := pw.Write(jpg); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
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
// selection happens via which /stream/*.mjpeg URL a client requests.
func (s *Server) handleSelect(w http.ResponseWriter, r *http.Request) {
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMain)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stream": stream.String()})
}

// handleOSD implements GET /osd?camera_id=<bool>&time=<bool>&enabled=<bool>.
func (s *Server) handleOSD(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if v, ok := parseBoolParam(q.Get("enabled")); ok {
		s.OSDCameraID.Store(v)
		s.OSDTime.Store(v)
	}
	if v, ok := parseBoolParam(q.Get("camera_id")); ok {
		s.OSDCameraID.Store(v)
	}
	if v, ok := parseBoolParam(q.Get("time")); ok {
		s.OSDTime.Store(v)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                true,
		"camera_id_enabled": s.OSDCameraID.Load(),
		"time_enabled":      s.OSDTime.Load(),
	})
}

// handleAnnotate implements GET /annotate?lores=<bool>&main=<bool>.
func (s *Server) handleAnnotate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if v, ok := parseBoolParam(q.Get("lores")); ok {
		s.LoresAnnotated.Store(v)
	}
	if v, ok := parseBoolParam(q.Get("main")); ok {
		s.MainAnnotated.Store(v)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"lores_annotated": s.LoresAnnotated.Load(),
		"main_annotated":  s.MainAnnotated.Load(),
	})
}

// handleCamera implements GET /camera?id=<N>, proxying picam-raw's raw
// JSON response through verbatim.
func (s *Server) handleCamera(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" || strings.ContainsFunc(idStr, func(c rune) bool { return c < '0' || c > '9' }) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	id, _ := strconv.Atoi(idStr)

	reached, resp := camrpc.SwitchCamera(s.cfg.PicamRawHost, s.cfg.PicamRawCmdPort, id)
	status := http.StatusOK
	if !reached {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(resp))
}

func round1(v float32) float64 {
	return math.Round(float64(v)*10) / 10
}

// handleStatusJSON implements GET /status.json.
func (s *Server) handleStatusJSON(w http.ResponseWriter, r *http.Request) {
	total, mainClients, loresClients := s.ClientCounts()
	snap := s.status.Snapshot()
	tel := s.telemetry.Snapshot()

	writeJSON(w, http.StatusOK, map[string]any{
		"clients":           total,
		"camera_id_enabled": s.OSDCameraID.Load(),
		"time_enabled":      s.OSDTime.Load(),
		"lores_annotated":   s.LoresAnnotated.Load(),
		"main_annotated":    s.MainAnnotated.Load(),
		"frame_ts_us":       snap.LatestFrameTsUs,
		"streams": map[string]int{
			"main":  mainClients,
			"lores": loresClients,
		},
		"telemetry": map[string]any{
			"connected":     tel.Connected,
			"lux":           round1(tel.Lux),
			"active_camera": tel.ActiveCamera,
			"camera_label":  tel.CameraLabel,
		},
	})
}

// handleDebugFrame implements GET /debug/frame.jpg?stream=main|lores. It
// JPEG-encodes the current live frame directly from the mailbox,
// bypassing the normal encode/broadcast path, so a headless box can
// `curl` it to see whether the frame feeding the encoder is already
// corrupt.
func (s *Server) handleDebugFrame(w http.ResponseWriter, r *http.Request) {
	if s.cfg.DebugFrameJPEG == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "debug frame endpoint disabled"})
		return
	}
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMain)
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
	stream := ParseStream(r.URL.Query().Get("stream"), StreamMain)
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
