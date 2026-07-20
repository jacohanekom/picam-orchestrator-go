# picam-orchestrator (Go)

A Go reimplementation of [`picam-orchestrator`](../picam-orchestrator) — a headless streaming backend for Raspberry Pi camera systems. Receives raw YUV420 video from `picam-raw` and object detection data from `picam-hailo`, then encodes to MJPEG and serves annotated or live video as a plain HTTP `multipart/x-mixed-replace` stream, directly usable as an `<img src="...">` — no WebRTC signaling. Same config file format and detection/telemetry/recorder wire protocols as the original C++ implementation; the live-video transport has since diverged (see below).

## Why MJPEG-over-HTTP instead of WebRTC/VP8

This started as a WebRTC/VP8 port (same shape as the original C++ implementation). VP8 has no hardware encode path on any Raspberry Pi, and software VP8 encoding (motion estimation, inter-frame prediction) was overloading the CPU on the actual deployment hardware. JPEG has neither — every frame is encoded independently — so it's inherently far cheaper even in pure software, and dropping WebRTC's ICE/DTLS/SFU machinery in favor of one plain HTTP connection per viewer removes a large chunk of both CPU cost and code. The trade-off: no built-in adaptive bitrate/resolution switching (there's no RTCP-equivalent feedback channel to adapt from) and no sub-second WebRTC-grade latency — both judged not worth the cost on this hardware. See `internal/mjpegsrv`'s package doc for more.

This port uses:

- **Go's standard `image/jpeg`** for both event snapshot files and the live MJPEG streams — it already encodes `image.YCbCr` directly in 4:2:0 without an RGB round-trip.
- **`net/http` + `mime/multipart`** for the live stream transport (`multipart.Writer` handles the `multipart/x-mixed-replace` framing) — no external dependencies at all for this.
- **`encoding/json`** for the detection/telemetry wire protocols, instead of a hand-rolled brace-counting scanner.

Everything else — the UDP chunk-reassembly protocol, delay buffer, detection buffer, annotation/OSD pixel drawing, camera-switch/recorder TCP control protocols, and the plain-text status protocol — is a direct behavioral port of the C++ original.

## Requirements

**Build:**
- Go 1.22+
- Nothing else — pure Go, no cgo, no system library dependencies.

**Runtime:**
- `picam-raw` (UDP streams + telemetry + command server)
- `picam-hailo` (detection TCP stream)
- `picam-frontend` (typical MJPEG client, relaying the stream on to browsers — though nothing stops a browser from pointing straight at this process's stream URLs)
- `picam-recorder` (optional — only needed for detection-triggered recording)

## Build

```bash
go build -o picam-orchestrator ./cmd/picam-orchestrator
```

No network access is needed at build time beyond the initial `go mod download`. No cgo, no system libraries to link against.

## Install (Debian package)

```bash
dpkg -i picam-orchestrator_*.deb
systemctl enable --now picam-orchestrator
```

The package creates a `picam-orchestrator` system user, installs the systemd unit, and deploys a default `config.ini` to `/etc/picam-orchestrator/`.

### From the APT repository

CI publishes to a signed APT repository (shared with other aipicam Raspberry Pi packages) hosted on Cloudflare R2, with two channels:

- **`main`** — pushing a `v*` tag publishes the clean release version here.
- **`nightly`** — every push (to any branch, and PRs) publishes a dev build here, versioned with a `+<UTC timestamp>` suffix.

```bash
curl -fsSL https://apt.aipicam.com/pubkey.asc | sudo gpg --dearmor -o /usr/share/keyrings/aipicam.gpg

# stable releases
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://apt.aipicam.com main main" | sudo tee /etc/apt/sources.list.d/aipicam.list

# or nightly builds instead
echo "deb [signed-by=/usr/share/keyrings/aipicam.gpg] https://apt.aipicam.com nightly main" | sudo tee /etc/apt/sources.list.d/aipicam.list

sudo apt-get update
sudo apt-get install picam-orchestrator
```

Builds run on GitHub's native `ubuntu-24.04-arm` hosted runner (no QEMU) so `go test` can actually execute the compiled arm64 test binaries. Uses the same `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `GPG_PRIVATE_KEY`, and `GPG_KEY_ID` repo secrets described in [pi-block-cpu-cores](../pi-block-cpu-cores)'s README, since it publishes into the same shared repo.

## Usage

```bash
./picam-orchestrator --config config.ini
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config`, `-c` | `config.ini` | Path to configuration file |

The HTTP server is available at `http://<pi-ip>:81` once the upstream services are running (see `/stream/*.mjpeg` below — this process never serves a browser-facing HTML page itself, just the raw MJPEG stream and JSON/control endpoints).

## Configuration

Same `config.ini` format and defaults as the C++ original (hand-rolled INI parser: `[section]` headers, `key = value` pairs, `;`/`#` comments). See [`config.ini`](config.ini) in this directory for the full annotated file, or the C++ project's README for the section-by-section rationale. All settings are read once at startup; annotation and OSD toggles can additionally be changed at runtime via the HTTP endpoints below.

## HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /stream/main.mjpeg` | Live main-stream video, `multipart/x-mixed-replace` MJPEG — usable directly as `<img src="...">` |
| `GET /stream/lores.mjpeg` | Live lores-stream video, same format |
| `/status.json` | Pipeline stats, FPS, client count, telemetry |
| `/annotate?main=true\|false&lores=true\|false` | Toggle delayed+annotated mode per resolution |
| `/osd?camera_id=true\|false&time=true\|false` | Toggle OSD overlays at runtime |
| `/camera?id=N` | Switch active camera (proxied to picam-raw) |
| `/select?stream=main\|lores` | Validates/echoes a stream name for client/UI sync (real per-viewer selection happens via which `/stream/*.mjpeg` URL is requested) |
| `/debug/frame.jpg?stream=main\|lores` | JPEG-encodes the current live frame straight from the mailbox, bypassing the normal encode/broadcast path — diagnostic |
| `/debug/frame.raw?stream=main\|lores` | Raw I420 bytes of the current live frame, no re-encode, with `X-Frame-*` headers reporting width/height/length — diagnostic |

`/stream/*.mjpeg` connections stay open (streaming) until the client disconnects; typically called by `picam-frontend`, though any HTTP client (including a browser directly) can connect. Every response (including errors) carries `Access-Control-Allow-Origin: *`; an unmatched route returns `404 text/plain "Not found"`.

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
picam-raw  ─────(UDP YUV420)────► picam-orchestrator ──(MJPEG/HTTP)──► picam-frontend ──► browsers
picam-hailo ────(TCP JSON)──────►        │
picam-recorder ◄──(TCP control)──────────┤
                                         ▼
                        GET /stream/main.mjpeg | /stream/lores.mjpeg
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
| `internal/imgscale` | Box-filter I420 downscale, used to cap the main stream's web-display resolution below its capture resolution |
| `internal/snapshot` | YUV420→JPEG, shared by event snapshot files and the live MJPEG streams (stdlib `image/jpeg`) |
| `internal/pipestat` | Shared pipeline counters read by both status endpoints |
| `internal/mjpegsrv` | MJPEG-over-HTTP streaming, client management, control endpoints, `/status.json` |
| `internal/statussrv` | Plain-text TCP status protocol |
| `cmd/picam-orchestrator` | Startup wiring and the main encode loop |

### Threading model

Each network-facing component (`rawframe.Receiver`, `detect.Run`, `telemetry.Run`, `recorder.EventRecorder`) runs on its own goroutine(s), all cancelled via a single `context.Context` cancelled on SIGINT/SIGTERM (`signal.NotifyContext`). JPEG encoding (`snapshot.Encode`) is stateless — no persistent encoder object, unlike VP8's inter-frame prediction requirement — so it's simply called inline from the single main-loop goroutine, once per resolution per tick. The MJPEG client list (`mjpegsrv.Server.clients`) is a copy-on-write `atomic.Pointer[[]*Client]`: the hot per-tick broadcast path does a single atomic load and never takes a lock, while register/prune (rare — only on viewer connect/disconnect) rebuild and atomically publish a fresh slice. Each client has its own small buffered channel + a per-request goroutine (the `GET /stream/*.mjpeg` handler itself) writing multipart parts to the HTTP response, so one slow/stalled client can't block the encoder or any other client — a dropped frame has no effect on any other frame, unlike VP8 where it broke the prediction chain until the next keyframe.

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
