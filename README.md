# picam-orchestrator (Go)

A from-scratch Go reimplementation of [`picam-orchestrator`](../picam-orchestrator) — a headless WebRTC streaming backend for Raspberry Pi camera systems. Receives raw YUV420 video from `picam-raw` and object detection data from `picam-hailo`, then encodes to VP8 and streams annotated or live video over WebRTC. Same wire protocols, config file format, and HTTP/TCP endpoint surface as the original C++ implementation — see that project's README for the full protocol-level rationale; this one focuses on what's specific to the Go port.

Main streams at its native capture resolution (no downscale) as two simultaneous, independently-bitrated VP8 encodes of the same frame — `main-high`/`main-low` — so `picam-frontend` can move a struggling browser viewer to a lower bitrate without ever dropping below native resolution (see [Architecture](#architecture)). Lores is unrelated to that — a third, always-available, always-native-lores-resolution stream, used unconditionally for grid-view overview thumbnails regardless of connection quality. This process itself does no adaptation: every stream it serves is flat and pinned to whatever a client explicitly requested for the life of that connection — real connection-quality adaptation lives one hop further out, in `picam-frontend`, which has the actual variable-quality link (browser↔frontend); this process's own link to `picam-frontend` is LAN-only and effectively always clean.

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
- `pi-relay-control` (optional, running locally on the same Pi — only needed for automatic IR-light relay control, see `[ir_light]`)

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

Builds run on GitHub's native `ubuntu-24.04-arm` hosted runner (no QEMU) so the cgo build against libvpx links against genuine native arm64 headers/libs. Uses the same `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `GPG_PRIVATE_KEY`, and `GPG_KEY_ID` repo secrets described in [pi-block-cpu-cores](../pi-block-cpu-cores)'s README, since it publishes into the same shared repo.

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
| `POST /webrtc/offer?stream=main\|main-low\|lores` | WHEP-style signaling — body `{"sdp":"..."}` (SDP offer), response `{"sdp":"..."}` (SDP answer). Flat/pinned: whichever stream is requested is what that connection gets for its whole lifetime, no server-side adaptation (`main` is a friendly alias for `main-high`). |
| `/status.json` | Pipeline stats, FPS, client count (broken down into `main`/`main_high`/`main_low`/`lores`), telemetry |
| `/annotate?main=true\|false&lores=true\|false` | Toggle delayed+annotated mode per resolution (applies to both main tiers together); persisted, see below |
| `/osd?camera_id=true\|false&time=true\|false` | Toggle OSD overlays at runtime; persisted, see below |
| `/camera?id=N` | Switch active camera (proxied to picam-raw); persisted, see below |
| `/lux-switch?enabled=true\|false&threshold=N` | Configure automatic lens switching by ambient light — see below |
| `/ir-light?enabled=true\|false&threshold=N&max_on_minutes=N` | Configure automatic IR-illuminator control via relay — see below |
| `/select?stream=main\|main-low\|lores` | Validates/echoes a stream name for client/UI sync (real per-client selection happens via `/webrtc/offer`'s own `?stream=` param) |

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

# Enable auto lens switching at a lux threshold of 60
curl http://<pi-ip>:81/lux-switch?enabled=true&threshold=60

# Enable the IR light relay below 50 lux, capped at 30 continuous minutes
curl http://<pi-ip>:81/ir-light?enabled=true&threshold=50&max_on_minutes=30

# Check pipeline status (plaintext key=value)
echo status | nc <pi-ip> 8091
```

### Automatic lens switching by ambient light

`internal/luxswitch` runs a background loop that, when enabled, watches picam-raw's own lux telemetry and switches the active camera automatically — above the configured threshold uses camera 0, below it uses camera 1, with a deadband and a cooldown between switches so it doesn't flap right at the boundary. This is independent of any client: it keeps working correctly with no browser open, since the decision and the `/camera` RPC to picam-raw both happen inside this process.

`enabled`/`threshold` start from `[lux_switch]` in `config.ini`, but a runtime change via `GET /lux-switch` is **persisted to disk** (`state_dir`, default `/var/lib/picam-orchestrator`) and takes priority over the config file on the next start. `picam-frontend`'s sidebar is a remote control for this setting, not where the logic runs — see that project's README.

### Automatic infrared light by relay

`internal/irlight` runs the same kind of background loop as `internal/luxswitch` (own deadband + cooldown, independent of any client), but drives a relay wired to an IR illuminator instead of switching cameras — below the configured threshold (dark) the relay turns on, above it (bright) it turns off. The relay itself is controlled via [`pi-relay-control`](../pi-relay-control), a small daemon assumed to be running **locally on this same Pi** (`[ir_light].relay_host`/`relay_port`, default `127.0.0.1:7778`) — `internal/relayrpc` speaks its plain-text one-shot TCP protocol (`on`/`off`), the same shape as `internal/camrpc`'s own picam-raw protocol.

`max_on_minutes` caps how long the relay may stay continuously on, as a hardware safety limit independent of the lux reading. Once hit, the relay is forced off and **stays off for the rest of that dark period** — it only re-arms (allowed to turn on again) once lux rises back above the threshold (day) and then drops below it again, so a single dark session never gets more than one allotment. `0` disables the cap entirely.

`enabled`/`threshold`/`max_on_minutes` start from `[ir_light]` in `config.ini`, but a runtime change via `GET /ir-light` is **persisted to disk** (`state_dir`, default `/var/lib/picam-orchestrator/ir_light.json`) the same way `[lux_switch]` is. `picam-frontend`'s Settings page is a remote control for this setting, not where the logic runs.

### Persisted Settings-page state

`internal/uistate` persists every other control on `picam-frontend`'s Settings page too — OSD overlay (`/osd`), annotation (`/annotate`), and the active camera lens (`/camera`) — the same way `internal/luxswitch` already persists auto-switch's own settings: a small JSON file under `[ui_state].state_dir` (default `/var/lib/picam-orchestrator/ui_state.json`), written on every successful change, read at startup ahead of `[osd]`/`[annotate]`'s own config.ini defaults.

Unlike OSD/annotate (this process's own in-memory `atomic.Bool` fields, read on every encode tick — `uistate` is a write-through persistence side channel, never that hot-path source of truth), the active camera isn't something this process owns at all day-to-day: `/camera` just proxies to picam-raw, and picam-raw is the actual source of truth for which lens is live. So restoring it is a one-shot reconciliation at startup (`uistate.ReconcileActiveCamera`, launched as a background goroutine, up to a 30s wait for telemetry to connect): if a saved camera preference exists and picam-raw reports something different once telemetry connects, this process issues one `/camera`-equivalent RPC to restore it. In the common case — picam-raw's own hardware state didn't change just because this process restarted — this is a no-op.

### Automatic discovery by picam-frontend

`internal/discovery` advertises this process over mDNS/DNS-SD (Zeroconf/Bonjour, RFC 6762/6763) as `_picam-orchestrator._tcp.local.`, using [`libp2p/zeroconf`](https://github.com/libp2p/zeroconf). `picam-frontend` browses for this service type instead of reading a static `[pis]` list, so a Pi shows up automatically as long as both processes are on the same mDNS-reachable network segment (typically: same L2 broadcast domain/VLAN — mDNS doesn't cross routed subnets). `[discovery].name` becomes the short id picam-frontend uses in its `?pi=` URLs (defaults to this Pi's OS hostname), and `[discovery].label` is the display label shown in picam-frontend's UI (defaults to the same value as `name`), carried as a TXT record. Set `[discovery].enabled = false` to opt a Pi out of discovery entirely.

## Architecture

```
picam-raw  ─────(UDP YUV420)────► picam-orchestrator ──(WebRTC/VP8: main-high, main-low, lores)──► picam-frontend ──► browsers
picam-hailo ────(TCP JSON)──────►        │
picam-recorder ◄──(TCP control)──────────┤
                                         ▼
                            POST /webrtc/offer (WHEP-style signaling)
```

picam-frontend maintains up to three separate upstream WebRTC connections per Pi (`main-high`, `main-low`, `lores`), lazily establishing only the ones a currently-connected browser actually needs, and moves each browser viewer between `main-high`/`main-low` based on that viewer's own downstream connection quality — see picam-frontend-go's README for that side of the adaptation.

### Package layout

| Package | Responsibility |
|---|---|
| `internal/config` | INI config parsing into a typed `Config` struct |
| `internal/rawframe` | UDP chunk reassembly, ping heartbeat, live-frame mailbox |
| `internal/delaybuffer` | Holds frames until `delay_ms` has elapsed |
| `internal/detect` | Detection JSON types, timestamp-indexed buffer, TCP receiver |
| `internal/telemetry` | Lux/active-camera TCP receiver + shared state |
| `internal/camrpc` | One-shot camera-switch TCP command to picam-raw |
| `internal/relayrpc` | One-shot on/off TCP command to pi-relay-control |
| `internal/luxswitch` | Automatic camera-lens switching by ambient light, persisted to disk |
| `internal/irlight` | Automatic IR-illuminator relay control by ambient light, persisted to disk |
| `internal/uistate` | Persists OSD/annotate/lens Settings-page state to disk, restoring lens on startup |
| `internal/discovery` | mDNS/DNS-SD advertisement so picam-frontend can find this Pi automatically |
| `internal/recorder` | picam-recorder TCP control + detection-triggered recording orchestration |
| `internal/annotate` | 5x7 bitmap font, Y-plane box/label drawing, OSD burn-in |
| `internal/snapshot` | YUV420→JPEG for event snapshot files (stdlib `image/jpeg`) |
| `internal/vp8` | cgo binding to libvpx for realtime VP8 encoding |
| `internal/pipestat` | Shared pipeline counters read by both status endpoints |
| `internal/webrtcsrv` | WHEP signaling, WebRTC client management, control endpoints, `/status.json` |
| `internal/statussrv` | Plain-text TCP status protocol |
| `cmd/picam-orchestrator` | Startup wiring and the main encode loop |

### Threading model

Each network-facing component (`rawframe.Receiver`, `detect.Run`, `telemetry.Run`, `recorder.EventRecorder`) runs on its own goroutine(s), all cancelled via a single `context.Context` cancelled on SIGINT/SIGTERM (`signal.NotifyContext`). The three `vp8.Encoder` instances (`main-high`, `main-low`, `lores`) are stateful and driven serially by the single main-loop goroutine — never called concurrently, matching VP8's inter-frame prediction requirement; a main tier is only encoded on ticks where it currently has at least one client. The WebRTC client list (`webrtcsrv.Server.clients`) is a copy-on-write `atomic.Pointer[[]*Client]`: the hot per-tick broadcast path does a single atomic load and never takes a lock, while register/prune (rare) rebuild and atomically publish a fresh slice. Each client has its own small buffered channel + writer goroutine feeding `TrackLocalStaticSample.WriteSample`, so one slow/stalled client can't block the encoder or any other client. Unlike an earlier version of this server, a `Client`'s stream is fixed at connect time and never adapted server-side — see the top of the README for why.

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
