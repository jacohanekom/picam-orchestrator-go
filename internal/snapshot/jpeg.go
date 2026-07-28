// Package snapshot JPEG-encodes YUV420 (I420) frames -- used both for
// EventRecorder's saved snapshot files and, at a per-stream quality
// setting, for the live MJPEG stream itself (see cmd/picam-orchestrator's
// runMainLoop and internal/streamsrv).
package snapshot

/*
#cgo pkg-config: libturbojpeg
#include <turbojpeg.h>

// compressYUVPlanes builds the srcPlanes/strides arrays tjCompressFromYUVPlanes
// wants on the C side, as local (stack) arrays. Building that array of
// pointers in Go instead -- e.g. a Go [3]*C.uchar -- would be a Go pointer
// (the array) to Go pointers (its elements, into the yuv slice's backing
// array), which cgo's pointer-passing rules forbid and its runtime checks
// panic on ("cgo argument has Go pointer to Go pointer"). Each of y/cb/cr
// individually is a plain single-level Go-pointer-to-C argument, which is
// fine.
static int compressYUVPlanes(tjhandle handle,
                              unsigned char *y, unsigned char *cb, unsigned char *cr,
                              int width, int strideY, int strideC, int height,
                              unsigned char **jpegBuf, unsigned long *jpegSize,
                              int quality) {
	unsigned char *planes[3] = { y, cb, cr };
	int strides[3] = { strideY, strideC, strideC };
	return tjCompressFromYUVPlanes(handle, planes, width, strides, height,
	                                TJSAMP_420, jpegBuf, jpegSize, quality, 0);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// Encode JPEG-encodes a packed I420 buffer (Y plane w*h, then U and V
// planes at w/2*h/2 each) at the given quality (1-100).
//
// Uses libjpeg-turbo's TurboJPEG API (tjCompressFromYUVPlanes) rather
// than Go's stdlib image/jpeg: both take the Y/Cb/Cr planes directly
// with no RGB round-trip (the same trick the C++ original did by hand
// via libjpeg's raw-data API), but only libjpeg-turbo's SIMD-accelerated
// DCT/quantization/entropy coding is fast enough to encode native-
// resolution (main is 2304x1296) MJPEG tiers within a 30fps budget --
// the stdlib encoder alone couldn't keep up (see the "falling behind
// real time" log in runMainLoop).
func Encode(yuv []byte, w, h, quality int) ([]byte, error) {
	yLen := w * h
	cLen := (w / 2) * (h / 2)
	if len(yuv) < yLen+2*cLen {
		return nil, fmt.Errorf("snapshot: short buffer: got %d bytes, want >= %d", len(yuv), yLen+2*cLen)
	}

	handle := C.tjInitCompress()
	if handle == nil {
		return nil, fmt.Errorf("snapshot: tjInitCompress failed")
	}
	defer C.tjDestroy(handle)

	// jpegBuf/jpegSize start nil/0, telling TurboJPEG to allocate the
	// output buffer itself -- must be freed with tjFree, not C.free, in
	// case it came from a different allocator.
	var jpegBuf *C.uchar
	var jpegSize C.ulong
	ret := C.compressYUVPlanes(handle,
		(*C.uchar)(unsafe.Pointer(&yuv[0])),
		(*C.uchar)(unsafe.Pointer(&yuv[yLen])),
		(*C.uchar)(unsafe.Pointer(&yuv[yLen+cLen])),
		C.int(w), C.int(w), C.int(w/2), C.int(h),
		&jpegBuf, &jpegSize, C.int(quality))
	if ret != 0 {
		return nil, fmt.Errorf("snapshot: tjCompressFromYUVPlanes failed: %s", C.GoString(C.tjGetErrorStr2(handle)))
	}
	defer C.tjFree(jpegBuf)

	out := make([]byte, int(jpegSize))
	copy(out, unsafe.Slice((*byte)(unsafe.Pointer(jpegBuf)), int(jpegSize)))
	return out, nil
}
