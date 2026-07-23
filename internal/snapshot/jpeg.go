// Package snapshot JPEG-encodes YUV420 (I420) frames for EventRecorder's
// saved snapshot files — not the live WebRTC stream (see internal/vp8).
package snapshot

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
)

// Encode JPEG-encodes a packed I420 buffer (Y plane w*h, then U and V
// planes at w/2*h/2 each) at the given quality (1-100).
//
// Go's image/jpeg encoder special-cases *image.YCbCr and encodes it
// directly in YCbCr space without an RGB round-trip — the same
// optimization the C++ original achieved by hand via libjpeg's raw-data
// API, obtained here for free from the standard library.
func Encode(yuv []byte, w, h, quality int) ([]byte, error) {
	yLen := w * h
	cLen := (w / 2) * (h / 2)
	if len(yuv) < yLen+2*cLen {
		return nil, fmt.Errorf("snapshot: short buffer: got %d bytes, want >= %d", len(yuv), yLen+2*cLen)
	}

	img := &image.YCbCr{
		Y:              yuv[:yLen],
		Cb:             yuv[yLen : yLen+cLen],
		Cr:             yuv[yLen+cLen : yLen+2*cLen],
		YStride:        w,
		CStride:        w / 2,
		SubsampleRatio: image.YCbCrSubsampleRatio420,
		Rect:           image.Rect(0, 0, w, h),
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
