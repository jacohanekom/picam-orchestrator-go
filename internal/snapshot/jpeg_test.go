package snapshot

import (
	"bytes"
	"image/jpeg"
	"testing"
)

// makeI420 builds a packed I420 buffer of a flat mid-gray frame with a
// bright square in the top-left corner, so decode can check both a
// smooth region and an edge without needing a real camera frame.
func makeI420(w, h int) []byte {
	yLen := w * h
	cLen := (w / 2) * (h / 2)
	buf := make([]byte, yLen+2*cLen)
	for i := 0; i < yLen; i++ {
		buf[i] = 128
	}
	for y := 0; y < h/4; y++ {
		for x := 0; x < w/4; x++ {
			buf[y*w+x] = 235
		}
	}
	for i := yLen; i < yLen+2*cLen; i++ {
		buf[i] = 128
	}
	return buf
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	const w, h = 64, 64
	yuv := makeI420(w, h)

	for _, quality := range []int{40, 85} {
		jpg, err := Encode(yuv, w, h, quality)
		if err != nil {
			t.Fatalf("Encode(quality=%d) failed: %v", quality, err)
		}
		if len(jpg) == 0 {
			t.Fatalf("Encode(quality=%d) returned empty output", quality)
		}

		img, err := jpeg.Decode(bytes.NewReader(jpg))
		if err != nil {
			t.Fatalf("decoding Encode(quality=%d) output failed: %v", quality, err)
		}
		bounds := img.Bounds()
		if bounds.Dx() != w || bounds.Dy() != h {
			t.Fatalf("Encode(quality=%d) round-trip size = %dx%d, want %dx%d",
				quality, bounds.Dx(), bounds.Dy(), w, h)
		}

		// Smooth mid-gray region, away from the bright square and any
		// block edges, should survive lossy compression closely.
		r, g, b, _ := img.At(w-1, h-1).RGBA()
		gray := (r>>8 + g>>8 + b>>8) / 3
		if diff := int(gray) - 128; diff < -20 || diff > 20 {
			t.Errorf("Encode(quality=%d) round-trip gray pixel = %d, want close to 128", quality, gray)
		}
	}
}

func TestEncodeShortBuffer(t *testing.T) {
	const w, h = 64, 64
	short := make([]byte, w*h) // no chroma planes
	if _, err := Encode(short, w, h, 80); err == nil {
		t.Fatal("Encode with a too-short buffer succeeded, want an error")
	}
}
