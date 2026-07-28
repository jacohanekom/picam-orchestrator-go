package recorder

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/at-wat/ebml-go/webm"
)

// bufferedFrame is one VP8 packet held in the rolling pre-buffer, or
// written straight into an open WebM file while recording/draining.
type bufferedFrame struct {
	data        []byte
	timestampUs int64
	wallTime    time.Time
	keyframe    bool
}

// rollingBuffer holds the last secs worth of VP8 frames, oldest first,
// trimming by wall-clock age on every push -- same shape as
// picam-recorder's own RollingBuffer, just holding VP8 packets instead
// of already-muxed NALUs.
type rollingBuffer struct {
	mu    sync.Mutex
	secs  float64
	items []bufferedFrame
}

func newRollingBuffer(secs float64) *rollingBuffer {
	return &rollingBuffer{secs: secs}
}

func (b *rollingBuffer) push(f bufferedFrame) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, f)
	cutoff := time.Now().Add(-time.Duration(b.secs * float64(time.Second)))
	i := 0
	for i < len(b.items) && b.items[i].wallTime.Before(cutoff) {
		i++
	}
	if i > 0 {
		b.items = append([]bufferedFrame(nil), b.items[i:]...)
	}
}

// drain returns a snapshot of every currently buffered frame (oldest
// first) and empties the buffer.
func (b *rollingBuffer) drain() []bufferedFrame {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.items
	b.items = nil
	return out
}

type recState int

const (
	stateIdle recState = iota
	stateRecording
	stateDraining
)

// Recorder captures picam-orchestrator's own already-encoded VP8 stream
// (see cmd/picam-orchestrator/main.go's mainEncoderRecord) into WebM
// files with a rolling pre-buffer and a post-stop drain window,
// replacing what picam-recorder used to do as a separate process. Since
// OnFrame is only ever called from runMainLoop's single goroutine, all
// the actual frame handling below is synchronous and lock-free from its
// perspective; mu only guards the fields Start/Stop (called from
// EventRecorder's own goroutine) touch concurrently with it.
type Recorder struct {
	dir      string
	width    int
	height   int
	fps      int
	preSecs  float64
	postSecs float64

	pre *rollingBuffer

	mu      sync.Mutex
	state   recState
	current string
	writer  webm.BlockWriteCloser
	baseUs  int64
	stopAt  time.Time
}

// NewRecorder prepares a video Recorder that writes into dir. width/height
// must match the resolution mainEncoderRecord actually encodes at (native
// main resolution). fps is used only to set the WebM track's advertised
// default frame duration.
func NewRecorder(dir string, width, height, fps int, preSecs, postSecs float64) *Recorder {
	return &Recorder{
		dir:      dir,
		width:    width,
		height:   height,
		fps:      fps,
		preSecs:  preSecs,
		postSecs: postSecs,
		pre:      newRollingBuffer(preSecs),
	}
}

// isKeyframe reports whether a VP8 frame (as returned by internal/vp8's
// Encoder.Encode) is a keyframe, per the VP8 bitstream's own frame tag:
// bit 0 of the first byte is 0 for a keyframe, 1 for an interframe. This
// is checked directly rather than trusting the forceKeyframe flag passed
// into Encode, since libvpx also self-inserts periodic keyframes
// independently of that flag.
func isKeyframe(vp8Bytes []byte) bool {
	return len(vp8Bytes) > 0 && vp8Bytes[0]&0x01 == 0
}

// OnFrame is called once per encoded frame from runMainLoop, always on
// the same goroutine. It always feeds the rolling pre-buffer, and additionally
// writes into the currently open WebM file if a recording is in progress or
// draining. A drain that has run for at least postSecs is finalized here too
// -- there's no need for a dedicated timer goroutine since this is already
// called every tick.
func (r *Recorder) OnFrame(vp8Bytes []byte, timestampUs int64) {
	if len(vp8Bytes) == 0 {
		return
	}
	f := bufferedFrame{
		data:        append([]byte(nil), vp8Bytes...),
		timestampUs: timestampUs,
		wallTime:    time.UnixMicro(timestampUs),
		keyframe:    isKeyframe(vp8Bytes),
	}
	r.pre.push(f)

	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case stateRecording, stateDraining:
		r.writeLocked(f)
	}
	if r.state == stateDraining && time.Since(r.stopAt) >= time.Duration(r.postSecs*float64(time.Second)) {
		r.finalizeLocked()
	}
}

func (r *Recorder) writeLocked(f bufferedFrame) {
	if r.writer == nil {
		return
	}
	if r.baseUs == 0 {
		r.baseUs = f.timestampUs
	}
	relMs := (f.timestampUs - r.baseUs) / 1000
	if relMs < 0 {
		relMs = 0
	}
	if _, err := r.writer.Write(f.keyframe, relMs, f.data); err != nil {
		fmt.Fprintf(os.Stderr, "[rec] WebM write error on %s: %v\n", r.current, err)
	}
}

// Start begins (or resumes) a recording named name, returning the output
// .webm path, or "" on failure. Idempotent while already recording;
// cancels an in-progress drain and resumes if called while draining.
func (r *Recorder) Start(name string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == stateRecording {
		return r.current
	}
	if r.state == stateDraining {
		r.state = stateRecording
		return r.current
	}

	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[rec] Cannot create %s: %v\n", r.dir, err)
		return ""
	}
	path := filepath.Join(r.dir, name+".webm")
	if _, err := os.Stat(path); err == nil {
		fmt.Fprintf(os.Stderr, "[rec] File already exists: %s\n", path)
		return ""
	}

	// Trim the pre-buffer to start at its first keyframe -- a WebM/VP8
	// stream must start on a keyframe to be decodable from the
	// beginning, and starting at the *first* one (rather than the
	// latest) keeps as much pre-roll as possible.
	frames := r.pre.drain()
	start := 0
	for start < len(frames) && !frames[start].keyframe {
		start++
	}
	frames = frames[start:]

	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[rec] Cannot create %s: %v\n", path, err)
		return ""
	}

	var defaultDurationNs uint64
	if r.fps > 0 {
		defaultDurationNs = uint64(time.Second) / uint64(r.fps)
	}
	ws, err := webm.NewSimpleBlockWriter(f, []webm.TrackEntry{
		{
			Name:            "Video",
			TrackNumber:     1,
			TrackUID:        1,
			CodecID:         "V_VP8",
			TrackType:       1,
			DefaultDuration: defaultDurationNs,
			Video: &webm.Video{
				PixelWidth:  uint64(r.width),
				PixelHeight: uint64(r.height),
			},
		},
	})
	if err != nil || len(ws) != 1 {
		fmt.Fprintf(os.Stderr, "[rec] Cannot open WebM writer for %s: %v\n", path, err)
		f.Close()
		os.Remove(path)
		return ""
	}

	r.writer = ws[0]
	r.current = path
	r.state = stateRecording
	r.baseUs = 0 // set lazily by writeLocked from the first frame actually written
	for _, bf := range frames {
		r.writeLocked(bf)
	}

	fmt.Printf("[rec] Recording started: %s (pre-buffer: %d frames)\n", path, len(frames))
	return path
}

// Stop requests the current recording stop after draining postSecs more
// of footage, returning the recording's path. No-op (returns "") if
// idle; idempotent if already draining.
func (r *Recorder) Stop() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stateRecording {
		return ""
	}
	r.state = stateDraining
	r.stopAt = time.Now()
	fmt.Printf("[rec] Stop requested — draining %.0fs post-buffer: %s\n", r.postSecs, r.current)
	return r.current
}

// finalizeLocked closes the current recording's WebM writer/file and
// returns to idle. Must be called with mu held.
func (r *Recorder) finalizeLocked() {
	path := r.current
	if r.writer != nil {
		if err := r.writer.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "[rec] WebM close error on %s: %v\n", path, err)
		}
	}
	r.writer = nil
	r.current = ""
	r.baseUs = 0
	r.state = stateIdle
	fmt.Printf("[rec] Recording finalized: %s\n", path)
}

// Status reports whether a recording (including its post-buffer drain)
// is in progress, and its path.
func (r *Recorder) Status() (recording bool, file string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state != stateIdle, r.current
}
