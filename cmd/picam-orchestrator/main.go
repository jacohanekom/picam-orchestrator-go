// Command picam-orchestrator is a Go port of the C++ picam-orchestrator
// service: it reassembles chunked UDP YUV420 frames from picam-raw,
// ingests JSON detection events from picam-hailo, optionally delays and
// annotates frames, encodes to VP8, and serves the result over WebRTC
// using WHEP-style signaling — plus plain HTTP/TCP control and status
// endpoints, and picam-recorder integration for detection-triggered
// recording. See picam-orchestrator-go/README.md for the full picture.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"picam-orchestrator/internal/annotate"
	"picam-orchestrator/internal/config"
	"picam-orchestrator/internal/delaybuffer"
	"picam-orchestrator/internal/detect"
	"picam-orchestrator/internal/pipestat"
	"picam-orchestrator/internal/rawframe"
	"picam-orchestrator/internal/recorder"
	"picam-orchestrator/internal/snapshot"
	"picam-orchestrator/internal/statussrv"
	"picam-orchestrator/internal/telemetry"
	"picam-orchestrator/internal/vp8"
	"picam-orchestrator/internal/webrtcsrv"
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

	status := &pipestat.Status{}
	telState := &telemetry.State{}
	detBuf := detect.New(cfg.DelayMs + cfg.ToleranceMs + 2000)

	var mainMailbox, loresMailbox rawframe.Mailbox
	mainDelayBuf := delaybuffer.New(cfg.DelayMs)
	loresDelayBuf := delaybuffer.New(cfg.DelayMs)

	// Diagnostic: JPEG-encode the current live frame for a stream straight
	// from its mailbox, bypassing VP8/WebRTC. curl GET /debug/frame.jpg
	// on a headless box to check whether the frame feeding the encoder is
	// already corrupt or whether corruption is introduced downstream.
	debugFrameJPEG := func(stream webrtcsrv.StreamSource) ([]byte, bool) {
		mb := &mainMailbox
		if stream == webrtcsrv.StreamLores {
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
	debugFrameRaw := func(stream webrtcsrv.StreamSource) ([]byte, int, int, bool) {
		mb := &mainMailbox
		if stream == webrtcsrv.StreamLores {
			mb = &loresMailbox
		}
		frame, ok := mb.Get()
		if !ok || len(frame.Data) == 0 {
			return nil, 0, 0, false
		}
		return frame.Data, frame.Width, frame.Height, true
	}

	srv, err := webrtcsrv.New(webrtcsrv.Config{
		HTTPPort:        cfg.HTTPPort,
		DefaultStream:   webrtcsrv.ParseStream(cfg.DefaultStream, webrtcsrv.StreamMain),
		ICEPortMin:      uint16(cfg.ICEPortMin),
		ICEPortMax:      uint16(cfg.ICEPortMax),
		PicamRawHost:    cfg.TelemetryHost,
		PicamRawCmdPort: cfg.CommandPort,
		MaxClients:      50,
		DebugFrameJPEG:  debugFrameJPEG,
		DebugFrameRaw:   debugFrameRaw,
	}, status, telState)
	if err != nil {
		log.Fatalf("[WebRTC] %v", err)
	}
	srv.OSDCameraID.Store(cfg.OSDCameraID)
	srv.OSDTime.Store(cfg.OSDTime)
	srv.MainAnnotated.Store(cfg.AnnotateMain)
	srv.LoresAnnotated.Store(cfg.AnnotateLores)

	// EventRecorder's snapshot callback: annotate a copy of the current
	// live MAIN frame with the triggering event's boxes and JPEG-encode
	// it. Always sourced from main's live mailbox, regardless of which
	// resolution's detections triggered the recording or whether main
	// annotation mode is currently on — matching the C++ original.
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
	evtRecorder := recorder.New(cfg.RecorderHost, cfg.RecorderPort, snapshotFn)

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

	mainEncoder, err := vp8.NewEncoder(cfg.MainWidth, cfg.MainHeight, cfg.VP8BitrateMainKbps, cfg.OutputFPSLive, cfg.VP8CPUUsedMain)
	if err != nil {
		log.Fatalf("[VP8] main encoder: %v", err)
	}
	defer mainEncoder.Close()
	loresEncoder, err := vp8.NewEncoder(cfg.LoresWidth, cfg.LoresHeight, cfg.VP8BitrateLoresKbps, cfg.OutputFPSLive, cfg.VP8CPUUsedLores)
	if err != nil {
		log.Fatalf("[VP8] lores encoder: %v", err)
	}
	defer loresEncoder.Close()

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
	runBg(func() { evtRecorder.Run(ctx) })

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

	runMainLoop(ctx, cfg, srv, status, detBuf, &mainMailbox, &loresMailbox, mainDelayBuf, loresDelayBuf, mainEncoder, loresEncoder)

	log.Printf("[Main] Shutting down.")
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
	log.Printf("[Config] encode      : vp8 main=%dkbps lores=%dkbps jpeg_quality=%d fps live=%d annotated=%d",
		cfg.VP8BitrateMainKbps, cfg.VP8BitrateLoresKbps, cfg.JPEGQuality, cfg.OutputFPSLive, cfg.OutputFPSAnnotated)
	log.Printf("[Config] webrtc      : ice_ports=%d-%d", cfg.ICEPortMin, cfg.ICEPortMax)
	log.Printf("[Config] annotate    : main=%v lores=%v", cfg.AnnotateMain, cfg.AnnotateLores)
	log.Printf("[Config] osd         : camera_id=%v time=%v", cfg.OSDCameraID, cfg.OSDTime)
	log.Printf("[Config] output      : http_port=%d status_port=%d default_stream=%s", cfg.HTTPPort, cfg.StatusPort, cfg.DefaultStream)
	log.Printf("[Config] recorder    : %s:%d", cfg.RecorderHost, cfg.RecorderPort)
}

// runMainLoop is a tick-for-tick port of the C++ original's main encode
// loop. See picam-orchestrator-go's plan doc for the full breakdown;
// notably it preserves (rather than "fixes") two quirks: frames_out
// increments by at most 1 per tick even if both resolutions encoded,
// and lores's frame timestamp wins as "newest" if both streams encode
// in the same tick (lores is evaluated second).
func runMainLoop(
	ctx context.Context,
	cfg *config.Config,
	srv *webrtcsrv.Server,
	status *pipestat.Status,
	detBuf *detect.Buffer,
	mainMailbox, loresMailbox *rawframe.Mailbox,
	mainDelayBuf, loresDelayBuf *delaybuffer.DelayBuffer,
	mainEncoder, loresEncoder *vp8.Encoder,
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

		total, mainClients, loresClients := srv.ClientCounts()

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
		if mainClients > 0 && now.Sub(lastMain).Microseconds() >= mainInterval {
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
				data := append([]byte(nil), frame.Data...)
				if mainAnnotated {
					if evt, ok := detBuf.FindNearest(frame.TimestampUs, toleranceUs); ok {
						annotate.DrawDetections(data, frame.Width, frame.Height, evt.Detections)
						matchedThisTick++
					}
				}
				if srv.OSDCameraID.Load() || srv.OSDTime.Load() {
					annotate.DrawOSD(data, frame.Width, frame.Height, frame.TimestampUs,
						cfg.CameraLabel(int(frame.CameraIndex)), srv.OSDCameraID.Load(), srv.OSDTime.Load())
				}
				forceKf := srv.ConsumeForceKeyframe(webrtcsrv.StreamMain)
				encStart := time.Now()
				vp8Bytes, err := mainEncoder.Encode(data, frame.TimestampUs, forceKf)
				if encDur := time.Since(encStart); encDur.Microseconds() > mainInterval {
					log.Printf("[VP8] main encode took %v, longer than the %v tick interval — "+
						"falling behind real time", encDur, time.Duration(mainInterval)*time.Microsecond)
				}
				if err != nil {
					log.Printf("[VP8] main encode error: %v", err)
				} else if len(vp8Bytes) > 0 {
					srv.Broadcast(webrtcsrv.StreamMain, vp8Bytes, now.Sub(lastMain))
				}
				lastMain = now
				didWork = true
				newestTsUs = frame.TimestampUs
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
						cfg.CameraLabel(int(frame.CameraIndex)), srv.OSDCameraID.Load(), srv.OSDTime.Load())
				}
				forceKf := srv.ConsumeForceKeyframe(webrtcsrv.StreamLores)
				if vp8Bytes, err := loresEncoder.Encode(data, frame.TimestampUs, forceKf); err != nil {
					log.Printf("[VP8] lores encode error: %v", err)
				} else if len(vp8Bytes) > 0 {
					srv.Broadcast(webrtcsrv.StreamLores, vp8Bytes, now.Sub(lastLores))
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
