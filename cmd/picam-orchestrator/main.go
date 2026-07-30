// Command picam-orchestrator is a Go port of the C++ picam-orchestrator
// service: it reassembles chunked UDP YUV420 frames from picam-raw,
// ingests JSON detection events from picam-hailo, optionally delays and
// annotates frames, JPEG-encodes them, and serves the result as MJPEG
// (multipart/x-mixed-replace) over plain HTTP — plus plain HTTP/TCP
// control and status endpoints, and picam-recorder integration for
// detection-triggered recording. See picam-orchestrator-go/README.md
// for the full picture.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"picam-orchestrator/internal/annotate"
	"picam-orchestrator/internal/config"
	"picam-orchestrator/internal/delaybuffer"
	"picam-orchestrator/internal/detect"
	"picam-orchestrator/internal/discovery"
	"picam-orchestrator/internal/irlight"
	"picam-orchestrator/internal/luxswitch"
	"picam-orchestrator/internal/pipestat"
	"picam-orchestrator/internal/rawframe"
	"picam-orchestrator/internal/recorder"
	"picam-orchestrator/internal/recorderstream"
	"picam-orchestrator/internal/snapshot"
	"picam-orchestrator/internal/statussrv"
	"picam-orchestrator/internal/streamsrv"
	"picam-orchestrator/internal/telemetry"
	"picam-orchestrator/internal/uistate"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.ini", "path to configuration file")
	flag.StringVar(&cfgPath, "c", "config.ini", "path to configuration file (shorthand)")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("[Config] %v", err)
	}
	logConfig(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	discSrv, err := discovery.Advertise(discovery.Config{
		Enabled:  cfg.DiscoveryEnabled,
		Name:     cfg.DiscoveryName,
		Label:    cfg.DiscoveryLabel,
		HTTPPort: cfg.HTTPPort,
	})
	if err != nil {
		log.Fatalf("[Discovery] %v", err)
	}

	status := &pipestat.Status{}
	telState := &telemetry.State{}
	detBuf := detect.New(cfg.DelayMs + cfg.ToleranceMs + 2000)

	var mainMailbox, loresMailbox rawframe.Mailbox
	mainDelayBuf := delaybuffer.New(cfg.DelayMs)
	loresDelayBuf := delaybuffer.New(cfg.DelayMs)

	// Latest already-JPEG-encoded main-high/main-low frame relayed from
	// picam-recorder's own GET /stream (see internal/recorderstream) --
	// runMainLoop broadcasts straight from these instead of self-
	// encoding whenever main is live (not annotated) and OSD is off; see
	// that function's own comment for why the other cases still self-
	// encode.
	var recorderMainHighJPEG, recorderMainLowJPEG jpegMailbox

	// Diagnostic: JPEG-encode the current live frame for a stream straight
	// from its mailbox as a single still image, with no client
	// registration. curl GET /debug/frame.jpg on a headless box to check
	// whether the frame feeding the live stream's own encode is already
	// corrupt or whether corruption is introduced downstream.
	debugFrameJPEG := func(stream streamsrv.StreamSource) ([]byte, bool) {
		mb := &mainMailbox
		if stream == streamsrv.StreamLores {
			mb = &loresMailbox
		}
		frame, ok := mb.Get()
		if !ok || len(frame.Data) == 0 {
			return nil, false
		}
		jpg, err := snapshot.Encode(frame.Data, frame.Width, frame.Height, cfg.JPEGQuality)
		if err != nil {
			return nil, false
		}
		return jpg, true
	}
	debugFrameRaw := func(stream streamsrv.StreamSource) ([]byte, int, int, bool) {
		mb := &mainMailbox
		if stream == streamsrv.StreamLores {
			mb = &loresMailbox
		}
		frame, ok := mb.Get()
		if !ok || len(frame.Data) == 0 {
			return nil, 0, 0, false
		}
		return frame.Data, frame.Width, frame.Height, true
	}

	luxState := luxswitch.New(
		filepath.Join(cfg.LuxSwitchStateDir, "lux_switch.json"),
		cfg.LuxSwitchEnabled, cfg.LuxSwitchThreshold,
	)
	uiState := uistate.New(
		filepath.Join(cfg.UIStateDir, "ui_state.json"),
		cfg.OSDCameraID, cfg.OSDTime, cfg.AnnotateMain, cfg.AnnotateLores,
	)
	irState := irlight.New(
		filepath.Join(cfg.IRLightStateDir, "ir_light.json"),
		cfg.IRLightEnabled, cfg.IRLightThreshold, cfg.IRLightMaxOnMinutes,
		cfg.IRLightSunriseEnabled, cfg.IRLightSunriseBeforeMinutes, cfg.IRLightSunriseAfterMinutes,
	)

	// EventRecorder's snapshot callback: annotate a copy of the current
	// live MAIN frame with the triggering event's boxes and JPEG-encode
	// it. Always sourced from main's live mailbox, regardless of which
	// resolution's detections triggered the recording or whether main
	// annotation mode is currently on — matching the C++ original.
	// Built (and evtRecorder constructed) ahead of streamsrv.New below so
	// it can be passed in for the manual GET /record trigger.
	snapshotFn := func(evt detect.Event) []byte {
		frame, ok := mainMailbox.Get()
		if !ok || len(frame.Data) == 0 {
			return nil
		}
		data := append([]byte(nil), frame.Data...)
		annotate.DrawDetections(data, frame.Width, frame.Height, evt.Detections)
		jpg, err := snapshot.Encode(data, frame.Width, frame.Height, cfg.JPEGQuality)
		if err != nil {
			log.Printf("[EventRecorder] snapshot encode failed: %v", err)
			return nil
		}
		return jpg
	}
	evtRecorder := recorder.New(cfg.RecorderHost, cfg.RecorderPort, cfg.RecorderIdleSecs, snapshotFn)

	srv, err := streamsrv.New(streamsrv.Config{
		HTTPPort:         cfg.HTTPPort,
		DefaultStream:    streamsrv.ParseStream(cfg.DefaultStream, streamsrv.StreamMainHigh),
		PicamRawHost:     cfg.TelemetryHost,
		PicamRawCmdPort:  cfg.CommandPort,
		MaxClients:       50,
		DebugFrameJPEG:   debugFrameJPEG,
		DebugFrameRaw:    debugFrameRaw,
		IRLightRelayHost: cfg.IRLightRelayHost,
		IRLightRelayPort: cfg.IRLightRelayPort,
		RecorderDir:      cfg.RecorderDir,
	}, status, telState, luxState, uiState, irState, evtRecorder)
	if err != nil {
		log.Fatalf("[HTTP] %v", err)
	}
	// Seeded from uiState's Snapshot (persisted value, if any, else the
	// [osd]/[annotate] config.ini defaults uiState was constructed
	// with) rather than cfg directly, so a prior runtime change survives
	// this restart.
	osdCameraID, osdTime, annotateMain, annotateLores, _ := uiState.Snapshot()
	srv.OSDCameraID.Store(osdCameraID)
	srv.OSDTime.Store(osdTime)
	srv.MainAnnotated.Store(annotateMain)
	srv.LoresAnnotated.Store(annotateLores)

	// The receiver callback hands each reassembled frame to both the live
	// mailbox and the delay buffer; the mailbox gets its own independent
	// copy of the pixel data so the two destinations never alias the same
	// backing array (mirroring the C++ original's copy-then-move split).
	mainReceiver := rawframe.New(rawframe.ReceiverConfig{
		Host: cfg.InputHost, Port: cfg.MainPort,
		Width: cfg.MainWidth, Height: cfg.MainHeight, PingEverySecs: cfg.PingEvery,
	}, func(f rawframe.RawFrame) {
		status.AddFramesIn()
		mailboxCopy := f
		mailboxCopy.Data = append([]byte(nil), f.Data...)
		mainMailbox.Set(mailboxCopy)
		mainDelayBuf.Push(f)
	})
	loresReceiver := rawframe.New(rawframe.ReceiverConfig{
		Host: cfg.InputHost, Port: cfg.LoresPort,
		Width: cfg.LoresWidth, Height: cfg.LoresHeight, PingEverySecs: cfg.PingEvery,
	}, func(f rawframe.RawFrame) {
		mailboxCopy := f
		mailboxCopy.Data = append([]byte(nil), f.Data...)
		loresMailbox.Set(mailboxCopy)
		loresDelayBuf.Push(f)
	})

	// When this process self-encodes main itself (annotated mode, or OSD
	// burned into the live view — see runMainLoop's own doc comment),
	// it's two independently-JPEG-quality encodes of the same native-
	// resolution frame, so picam-frontend can move a struggling browser
	// viewer between them (see internal/relay's quality-switching logic
	// in picam-frontend-go). In the common case (plain live, OSD off)
	// main is instead proxied straight through from picam-recorder's own
	// always-live compression, which downscales main-low for its own
	// sustained-throughput reasons — see that project's README. Unlike
	// VP8, JPEG has no persistent encoder state (no rate-control
	// history, no GOP, no keyframe scheduling) — each self-encoded frame
	// is just a stateless snapshot.Encode call at the tier's own quality
	// setting, called directly from runMainLoop below rather than
	// needing a constructed encoder object here.

	var wg sync.WaitGroup
	runBg := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}

	runBg(func() { statussrv.Run(ctx, cfg.StatusPort, status) })

	if err := mainReceiver.Start(ctx); err != nil {
		log.Fatalf("[UDP] main: %v", err)
	}
	if err := loresReceiver.Start(ctx); err != nil {
		log.Fatalf("[UDP] lores: %v", err)
	}

	runBg(func() {
		detect.Run(ctx, cfg.DetectionsHost, cfg.DetectionsPort, detBuf, evtRecorder.Notify)
	})
	runBg(func() {
		telemetry.Run(ctx, cfg.TelemetryHost, cfg.TelemetryPort, telState)
	})
	runBg(func() {
		luxswitch.Run(ctx, luxState, telState, cfg.TelemetryHost, cfg.CommandPort)
	})
	runBg(func() {
		irlight.Run(ctx, irState, telState, cfg.IRLightRelayHost, cfg.IRLightRelayPort, cfg.IRLightLatitude, cfg.IRLightLongitude)
	})
	// One-shot, not a loop like the Runs above: restores the last
	// selected lens if it differs from whatever picam-raw reports once
	// telemetry connects, then returns.
	runBg(func() {
		uistate.ReconcileActiveCamera(ctx, uiState, telState, cfg.TelemetryHost, cfg.CommandPort, 30*time.Second)
	})
	runBg(func() { evtRecorder.Run(ctx) })
	// Always connected, independent of client count or annotation/OSD
	// mode -- picam-recorder always publishes (see that project's
	// StreamServer), so this just keeps the latest frame of each tier
	// on hand for runMainLoop to broadcast whenever it's not
	// self-encoding main itself.
	runBg(func() {
		recorderstream.Run(ctx, cfg.RecorderHost, cfg.RecorderStreamPort, "main-high", recorderMainHighJPEG.Set)
	})
	runBg(func() {
		recorderstream.Run(ctx, cfg.RecorderHost, cfg.RecorderStreamPort, "main-low", recorderMainLowJPEG.Set)
	})

	srv.Start()

	log.Printf("[Main] Waiting for main stream...")
	if !mainReceiver.WaitForStream(ctx, 30*time.Second) {
		log.Printf("[Main] WARNING: no main stream frames received within 30s")
	}
	log.Printf("[Main] Waiting for lores stream...")
	if !loresReceiver.WaitForStream(ctx, 10*time.Second) {
		log.Printf("[Main] WARNING: no lores stream frames received within 10s")
	}
	log.Printf("[Main] Streams active. Open http://<pi-ip>:%d", cfg.HTTPPort)

	runMainLoop(ctx, cfg, srv, status, telState, detBuf, &mainMailbox, &loresMailbox, mainDelayBuf, loresDelayBuf,
		&recorderMainHighJPEG, &recorderMainLowJPEG)

	log.Printf("[Main] Shutting down.")
	if discSrv != nil {
		discSrv.Shutdown()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	srv.Stop(shutdownCtx)
	shutdownCancel()
	mainReceiver.Wait()
	loresReceiver.Wait()
	wg.Wait()
}

func logConfig(cfg *config.Config) {
	log.Printf("[Config] input       : %s main=%dx%d:%d lores=%dx%d:%d ping_every=%ds",
		cfg.InputHost, cfg.MainWidth, cfg.MainHeight, cfg.MainPort, cfg.LoresWidth, cfg.LoresHeight, cfg.LoresPort, cfg.PingEvery)
	log.Printf("[Config] detections  : %s:%d tolerance_ms=%d", cfg.DetectionsHost, cfg.DetectionsPort, cfg.ToleranceMs)
	log.Printf("[Config] telemetry   : %s:%d command_port=%d", cfg.TelemetryHost, cfg.TelemetryPort, cfg.CommandPort)
	log.Printf("[Config] delay       : %dms (applied to whichever resolution has annotation on)", cfg.DelayMs)
	log.Printf("[Config] encode      : mjpeg main high=%d low=%d lores=%d (1-100) snapshot_jpeg_quality=%d fps live=%d annotated=%d",
		cfg.MJPEGQualityHigh, cfg.MJPEGQualityLow, cfg.MJPEGQualityLores, cfg.JPEGQuality, cfg.OutputFPSLive, cfg.OutputFPSAnnotated)
	log.Printf("[Config] annotate    : main=%v lores=%v", cfg.AnnotateMain, cfg.AnnotateLores)
	log.Printf("[Config] osd         : camera_id=%v time=%v", cfg.OSDCameraID, cfg.OSDTime)
	log.Printf("[Config] lux_switch  : enabled=%v threshold=%d state_dir=%s", cfg.LuxSwitchEnabled, cfg.LuxSwitchThreshold, cfg.LuxSwitchStateDir)
	log.Printf("[Config] ir_light    : enabled=%v threshold=%d max_on_minutes=%d relay=%s:%d state_dir=%s",
		cfg.IRLightEnabled, cfg.IRLightThreshold, cfg.IRLightMaxOnMinutes, cfg.IRLightRelayHost, cfg.IRLightRelayPort, cfg.IRLightStateDir)
	log.Printf("[Config] ir_light sunrise: enabled=%v before=%dm after=%dm lat=%.4f lon=%.4f",
		cfg.IRLightSunriseEnabled, cfg.IRLightSunriseBeforeMinutes, cfg.IRLightSunriseAfterMinutes, cfg.IRLightLatitude, cfg.IRLightLongitude)
	log.Printf("[Config] discovery   : enabled=%v name=%q label=%q", cfg.DiscoveryEnabled, cfg.DiscoveryName, cfg.DiscoveryLabel)
	log.Printf("[Config] ui_state    : state_dir=%s", cfg.UIStateDir)
	log.Printf("[Config] output      : http_port=%d status_port=%d default_stream=%s", cfg.HTTPPort, cfg.StatusPort, cfg.DefaultStream)
	log.Printf("[Config] recorder    : %s:%d control, :%d stream", cfg.RecorderHost, cfg.RecorderPort, cfg.RecorderStreamPort)
}

// runMainLoop is a tick-for-tick port of the C++ original's main encode
// loop. See picam-orchestrator-go's plan doc for the full breakdown;
// notably it preserves (rather than "fixes") two quirks: frames_out
// increments by at most 1 per tick even if both resolutions encoded,
// and lores's frame timestamp wins as "newest" if both streams encode
// in the same tick (lores is evaluated second).
//
// Main only self-encodes when it has to draw something into the pixels
// that only this process can (detection boxes in annotated mode, or the
// OSD overlay) -- otherwise it proxies picam-recorder's own always-live
// JPEG compression straight through via recorderMainHigh/Low (see
// internal/recorderstream and main()'s two relay goroutines) instead of
// re-compressing the exact same frames itself. Lores is unaffected --
// picam-recorder never touches it -- and always self-encodes exactly as
// before.
func runMainLoop(
	ctx context.Context,
	cfg *config.Config,
	srv *streamsrv.Server,
	status *pipestat.Status,
	telState *telemetry.State,
	detBuf *detect.Buffer,
	mainMailbox, loresMailbox *rawframe.Mailbox,
	mainDelayBuf, loresDelayBuf *delaybuffer.DelayBuffer,
	recorderMainHigh, recorderMainLow *jpegMailbox,
) {
	liveIntervalUs := fpsIntervalUs(cfg.OutputFPSLive)
	annotIntervalUs := fpsIntervalUs(cfg.OutputFPSAnnotated)
	toleranceUs := int64(cfg.ToleranceMs) * 1000

	lastMain := time.Now()
	lastLores := time.Now()
	lastHeartbeat := time.Now()

	for ctx.Err() == nil {
		now := time.Now()
		didWork := false
		var newestTsUs int64
		var matchedThisTick uint64

		total, mainHighClients, mainLowClients, loresClients := srv.ClientCounts()
		mainClients := mainHighClients + mainLowClients

		mainAnnotated := srv.MainAnnotated.Load()
		loresAnnotated := srv.LoresAnnotated.Load()

		// Always attempt a non-blocking pop per resolution, every tick,
		// regardless of annotation mode or client count — keeps each
		// delay buffer from growing unbounded and keeps it "warm" so
		// toggling annotation on doesn't have to wait to refill.
		mainFrame, mainPopped := mainDelayBuf.Pop()
		loresFrame, loresPopped := loresDelayBuf.Pop()

		// — Main —
		mainInterval := chooseInterval(mainAnnotated, liveIntervalUs, annotIntervalUs)
		mainSelfEncode := mainAnnotated || srv.OSDCameraID.Load() || srv.OSDTime.Load()
		if mainClients > 0 && now.Sub(lastMain).Microseconds() >= mainInterval {
			if mainSelfEncode {
				var frame rawframe.RawFrame
				haveFrame := false
				if mainAnnotated {
					if mainPopped {
						frame, haveFrame = mainFrame, true
					}
				} else {
					frame, haveFrame = mainMailbox.Get()
				}
				if haveFrame && len(frame.Data) > 0 {
					// Native resolution, no downscale — recording/snapshots
					// already ran at full native resolution unconditionally;
					// now the live view does too. The copy is still needed
					// since annotate below mutates in place and frame.Data
					// is shared with the mailbox/delay buffer.
					data := append([]byte(nil), frame.Data...)
					if mainAnnotated {
						if evt, ok := detBuf.FindNearest(frame.TimestampUs, toleranceUs); ok {
							annotate.DrawDetections(data, frame.Width, frame.Height, evt.Detections)
							matchedThisTick++
						}
					}
					if srv.OSDCameraID.Load() || srv.OSDTime.Load() {
						annotate.DrawOSD(data, frame.Width, frame.Height, frame.TimestampUs,
							cfg.CameraLabel(int(frame.CameraIndex)), telState.UtcOffsetMinutes(),
							srv.OSDCameraID.Load(), srv.OSDTime.Load())
					}
					// Only encode a tier that currently has at least one
					// client -- JPEG has no persistent encoder state, so
					// this is just a stateless snapshot.Encode call per
					// tier, each independent of the others.
					encStart := time.Now()
					if mainHighClients > 0 {
						if jpg, err := snapshot.Encode(data, frame.Width, frame.Height, cfg.MJPEGQualityHigh); err != nil {
							log.Printf("[MJPEG] main-high encode error: %v", err)
						} else if len(jpg) > 0 {
							srv.Broadcast(streamsrv.StreamMainHigh, jpg)
						}
					}
					if mainLowClients > 0 {
						if jpg, err := snapshot.Encode(data, frame.Width, frame.Height, cfg.MJPEGQualityLow); err != nil {
							log.Printf("[MJPEG] main-low encode error: %v", err)
						} else if len(jpg) > 0 {
							srv.Broadcast(streamsrv.StreamMainLow, jpg)
						}
					}
					if encDur := time.Since(encStart); encDur.Microseconds() > mainInterval {
						log.Printf("[MJPEG] main encode (both tiers) took %v, longer than the %v tick interval — "+
							"falling behind real time", encDur, time.Duration(mainInterval)*time.Microsecond)
					}
					lastMain = now
					didWork = true
					newestTsUs = frame.TimestampUs
				}
			} else {
				// Plain live, OSD off: proxy picam-recorder's own
				// already-compressed frames straight through, no encode
				// at all. No timestamp travels with a relayed JPEG, so
				// newestTsUs isn't updated here -- lores keeps it fresh
				// most ticks regardless (see this function's own "lores
				// wins" note above).
				broadcast := false
				if mainHighClients > 0 {
					if jpg, ok := recorderMainHigh.Get(); ok {
						srv.Broadcast(streamsrv.StreamMainHigh, jpg)
						broadcast = true
					}
				}
				if mainLowClients > 0 {
					if jpg, ok := recorderMainLow.Get(); ok {
						srv.Broadcast(streamsrv.StreamMainLow, jpg)
						broadcast = true
					}
				}
				if broadcast {
					lastMain = now
					didWork = true
				}
			}
		} else if mainPopped {
			didWork = true
		}

		// — Lores — (mirrors Main; if both fire this tick, lores is
		// evaluated second so its timestamp wins as "newest" — a
		// preserved C++ quirk, not fixed here.)
		loresInterval := chooseInterval(loresAnnotated, liveIntervalUs, annotIntervalUs)
		if loresClients > 0 && now.Sub(lastLores).Microseconds() >= loresInterval {
			var frame rawframe.RawFrame
			haveFrame := false
			if loresAnnotated {
				if loresPopped {
					frame, haveFrame = loresFrame, true
				}
			} else {
				frame, haveFrame = loresMailbox.Get()
			}
			if haveFrame && len(frame.Data) > 0 {
				data := append([]byte(nil), frame.Data...)
				if loresAnnotated {
					if evt, ok := detBuf.FindNearest(frame.TimestampUs, toleranceUs); ok {
						annotate.DrawDetections(data, frame.Width, frame.Height, evt.Detections)
						matchedThisTick++
					}
				}
				if srv.OSDCameraID.Load() || srv.OSDTime.Load() {
					annotate.DrawOSD(data, frame.Width, frame.Height, frame.TimestampUs,
						cfg.CameraLabel(int(frame.CameraIndex)), telState.UtcOffsetMinutes(),
						srv.OSDCameraID.Load(), srv.OSDTime.Load())
				}
				if jpg, err := snapshot.Encode(data, frame.Width, frame.Height, cfg.MJPEGQualityLores); err != nil {
					log.Printf("[MJPEG] lores encode error: %v", err)
				} else if len(jpg) > 0 {
					srv.Broadcast(streamsrv.StreamLores, jpg)
				}
				lastLores = now
				didWork = true
				newestTsUs = frame.TimestampUs
			}
		} else if loresPopped {
			didWork = true
		}

		sleepMs := 2 * time.Millisecond
		if didWork {
			sleepMs = 1 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepMs):
		}

		status.SetTick(mainDelayBuf.Size()+loresDelayBuf.Size(), total, matchedThisTick, newestTsUs)

		if time.Since(lastHeartbeat) >= time.Second {
			fmt.Fprintf(os.Stderr, "\r[Main] main=%s lores=%s buf=%d   ",
				modeStr(mainAnnotated), modeStr(loresAnnotated), mainDelayBuf.Size()+loresDelayBuf.Size())
			lastHeartbeat = now
		}
	}
}

func fpsIntervalUs(fps int) int64 {
	if fps <= 0 {
		return 0
	}
	return int64(1e6 / float64(fps))
}

func chooseInterval(annotated bool, liveUs, annotUs int64) int64 {
	if annotated {
		return annotUs
	}
	return liveUs
}

func modeStr(annotated bool) string {
	if annotated {
		return "annotated"
	}
	return "live"
}
