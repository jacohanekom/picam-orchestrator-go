# picam-orchestrator (Go)

A from-scratch Go reimplementation of [`picam-orchestrator`](../picam-orchestrator) — a headless WebRTC streaming backend for Raspberry Pi camera systems. Receives raw YUV420 video from `picam-raw` and object detection data from `picam-hailo`, then encodes to VP8 and streams annotated or live video over WebRTC. Same wire protocols, config file format, and HTTP/TCP endpoint surface as the original C++ implementation — see that project's README for the full protocol-level rationale; this one focuses on what's specific to the Go port.

## Why a Go port

The original C++ implementation vendors [libdatachannel](https://github.com/paullouisageneau/libdatachannel) via CMake `FetchContent` (needs network access at configure time) and links `libssl`/`libjpeg`/`libvpx`. This port instead uses:

- **[pion/webrtc](https://github.com/pion/webrtc)** (pure Go) for WebRTC/ICE/DTLS/SRTP and VP8 RTP packetization — no vendored C++ WebRTC stack, no OpenSSL build step. pion's `SetRemoteDescription`→`AddTrack`→`CreateAnswer` flow also sidesteps a mid/m-line-matching bug the C++ version had to hand-fix.
- **A small cgo binding directly to the system `libvpx`** (`internal/vp8`) for VP8 encoding — same realtime CBR config as the original (one-pass, no lookahead, forced-keyframes-only), since there's no mature pure-Go VP8 encoder.
- **Go's standard `image/jpeg`** for event snapshot files — it already encodes `image.YCbCr` directly in 4:2:0 without an RGB round-trip, which is exactly what the C++ version hand-rolled raw libjpeg calls to achieve.
- **`encoding/json`** for the detection/telemetry wire protocols, instead of a hand-rolled brace-counting scanner.

Everything else — the UDP chunk-reassembly protocol, delay buffer, detection buffer, annotation/OSD pixel drawing, camera-switch/recorder TCP control protocols, and the plain-text status protocol — is a direct behavioral port.

## Requirements

**Build:**
- Go 1.22+
- `pkg-config` and `libvpx-dev` (or `libvpx` + headers via Homebrew on macOS) — needed for the cgo VP8 encoder

**Runtime:**
- `libvpx` shared library
- `picam-raw` (UDP streams + telemetry + command server)
- `picam-hailo` (detection TCP stream)
- `picam-frontend` (the only WebRTC signaling/media client this process ever talks to)
- `picam-recorder` (optional — only needed for detection-triggered recording)

## Build

```bash
go build -o picam-orchestrator ./cmd/picam-orchestrator
```

No network access is needed at build time beyond the initial `go mod download` (all dependencies are pure Go except the cgo `libvpx` binding, which links against the system library via `pkg-config`).

## Install (Debian package)

```bash
dpkg -i picam-orchestrator_*.deb
systemctl enable --now picam-orchestrator
```

The package creates a `picam-orchestrator` system user, installs the systemd unit, and deploys a default `config.ini` to `/etc/picam-orchestrator/`.

## Usage

```bash
./picam-orchestrator --config config.ini
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config`, `-c` | `config.ini` | Path to configuration file |

The HTTP control server is available at `http://<pi-ip>:81` once the upstream services are running (see `POST /webrtc/offer` below — this process never serves a browser-facing page itself).

## Configuration

Same `config.ini` format and defaults as the C++ original (hand-rolled INI parser: `[section]` headers, `key = value` pairs, `;`/`#` comments). See [`config.ini`](config.ini) in this directory for the full annotated file, or the C++ project's README for the section-by-section rationale. All settings are read once at startup; annotation and OSD toggles can additionally be changed at runtime via the HTTP endpoints below.

## HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /webrtc/offer?stream=main\|lores` | WHEP-style signaling — body `{"sdp":"..."}` (SDP offer), response `{"sdp":"..."}` (SDP answer) |
| `/status.json` | Pipeline stats, FPS, client count, telemetry |
| `/annotate?main=true\|false&lores=true\|false` | Toggle delayed+annotated mode per resolution |
| `/osd?camera_id=true\|false&time=true\|false` | Toggle OSD overlays at runtime |
| `/camera?id=N` | Switch active camera (proxied to picam-raw) |
| `/select?stream=main\|lores` | Validates/echoes a stream name for client/UI sync (real per-client selection happens via `/webrtc/offer`'s own `?stream=` param) |

`/webrtc/offer` is meant to be called by `picam-frontend`, not a browser directly. Every response (including errors) carries `Access-Control-Allow-Origin: *`; an unmatched route returns `404 text/plain "Not found"`.

### Examples

```bash
# Enable annotated main stream (frames held delay_ms, boxes drawn)
curl http://<pi-ip>:81/annotate?main=true

# Disable annotation, return to zero-latency live
curl http://<pi-ip>:81/annotate?main=false

# Show timestamp OSD
curl http://<pi-ip>:81/osd?time=true

# Switch to camera 1
curl http://<pi-ip>:81/camera?id=1

# Check pipeline status (plaintext key=value)
echo status | nc <pi-ip> 8091
```

## Architecture

```
picam-raw  ─────(UDP YUV420)────► picam-orchestrator ──(WebRTC/VP8)──► picam-frontend ──► browsers
picam-hailo ────(TCP JSON)──────►        │
picam-recorder ◄──(TCP control)──────────┤
                                         ▼
                            POST /webrtc/offer (WHEP-style signaling)
```

### Package layout

| Package | Responsibility |
|---|---|
| `internal/config` | INI config parsing into a typed `Config` struct |
| `internal/rawframe` | UDP chunk reassembly, ping heartbeat, live-frame mailbox |
| `internal/delaybuffer` | Holds frames until `delay_ms` has elapsed |
| `internal/detect` | Detection JSON types, timestamp-indexed buffer, TCP receiver |
| `internal/telemetry` | Lux/active-camera TCP receiver + shared state |
| `internal/camrpc` | One-shot camera-switch TCP command to picam-raw |
| `internal/recorder` | picam-recorder TCP control + detection-triggered recording orchestration |
| `internal/annotate` | 5x7 bitmap font, Y-plane box/label drawing, OSD burn-in |
| `internal/snapshot` | YUV420→JPEG for event snapshot files (stdlib `image/jpeg`) |
| `internal/vp8` | cgo binding to libvpx for realtime VP8 encoding |
| `internal/pipestat` | Shared pipeline counters read by both status endpoints |
| `internal/webrtcsrv` | WHEP signaling, WebRTC client management, control endpoints, `/status.json` |
| `internal/statussrv` | Plain-text TCP status protocol |
| `cmd/picam-orchestrator` | Startup wiring and the main encode loop |

### Threading model

Each network-facing component (`rawframe.Receiver`, `detect.Run`, `telemetry.Run`, `recorder.EventRecorder`) runs on its own goroutine(s), all cancelled via a single `context.Context` cancelled on SIGINT/SIGTERM (`signal.NotifyContext`). The two `vp8.Encoder` instances (one per resolution) are stateful and driven serially by the single main-loop goroutine — never called concurrently, matching VP8's inter-frame prediction requirement. The WebRTC client list (`webrtcsrv.Server.clients`) is a copy-on-write `atomic.Pointer[[]*Client]`: the hot per-tick broadcast path does a single atomic load and never takes a lock, while register/prune (rare) rebuild and atomically publish a fresh slice. Each client has its own small buffered channel + writer goroutine feeding `TrackLocalStaticSample.WriteSample`, so one slow/stalled client can't block the encoder or any other client.

### Known, intentionally-preserved quirks

Carried over from the C++ original rather than "fixed," in case anything downstream depends on the exact behavior:

- The plain-text status protocol's `fps` field is always `0.0` — never actually computed in the original either.
- `frames_out` increments by at most 1 per main-loop tick even if both resolutions encoded a frame that tick.
- If both streams encode in the same tick, lores's frame timestamp wins as the tick's "newest" (lores is evaluated second).

## Status output

```
$ echo status | nc <pi-ip> 8091
ok=true
frames_in=1234
frames_out=1230
matched=1229
fps=0.0
delay_buffer_depth=2
clients=3
```

`/status.json` returns the same counters as JSON alongside telemetry (lux, active camera, label) and per-stream client counts.

## Systemd service

```bash
systemctl start   picam-orchestrator
systemctl stop    picam-orchestrator
systemctl status  picam-orchestrator
journalctl -u picam-orchestrator -f
```

The unit runs as an unprivileged user with `CAP_NET_BIND_SERVICE` (for port 81), pinned to CPU core 2, and restarts automatically after 3 seconds on failure.
