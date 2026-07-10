// Package vp8 wraps libvpx via cgo for realtime VP8 encoding of the
// WebRTC live stream. This mirrors the C++ original's Vp8Encoder
// exactly: one-pass CBR, no lookahead, error-resilient, periodic
// keyframes disabled (keyframes are only ever produced when explicitly
// forced).
package vp8

/*
#cgo pkg-config: vpx
#include <stdlib.h>
#include <vpx/vp8cx.h>
#include <vpx/vpx_encoder.h>
#include <vpx/vpx_image.h>

// vpx_codec_enc_init and vpx_codec_control are function-like macros in
// libvpx's public headers (the former injects VPX_ENCODER_ABI_VERSION,
// the latter is a variadic dispatch macro) — neither can be called
// directly from cgo, hence these tiny wrappers. Likewise, the
// compressed-frame packet's buf/sz live inside a C union
// (vpx_codec_cx_pkt_t.data.frame), which cgo cannot address field-by-
// field from Go, so a small accessor is used for that too.
static vpx_codec_err_t vp8_enc_init(vpx_codec_ctx_t *ctx, const vpx_codec_enc_cfg_t *cfg) {
    return vpx_codec_enc_init(ctx, vpx_codec_vp8_cx(), cfg, 0);
}

static vpx_codec_err_t vp8_set_cpuused(vpx_codec_ctx_t *ctx, int v) {
    return vpx_codec_control(ctx, VP8E_SET_CPUUSED, v);
}

static void vp8_frame_data(const vpx_codec_cx_pkt_t *pkt, const void **buf, size_t *sz) {
    *buf = pkt->data.frame.buf;
    *sz = pkt->data.frame.sz;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

// Encoder wraps a stateful libvpx VP8 encoder instance for one
// resolution. It is NOT safe for concurrent use: VP8 predicts each
// frame from the ones before it, so a single Encoder must be driven
// serially, in frame order, by exactly one goroutine — the same
// single-pipeline-thread invariant the C++ original documents. Construct
// one per resolution and reuse it across live/annotated mode switches.
type Encoder struct {
	ctx           C.vpx_codec_ctx_t
	img           C.vpx_image_t
	width, height int
	closed        bool
}

// NewEncoder configures and initializes a realtime CBR VP8 encoder at
// width x height, targeting bitrateKbps kilobits/sec; fps only seeds the
// VP8 timebase denominator (it does not change at runtime).
func NewEncoder(width, height, bitrateKbps, fps int) (*Encoder, error) {
	var cfg C.vpx_codec_enc_cfg_t
	if rc := C.vpx_codec_enc_config_default(C.vpx_codec_vp8_cx(), &cfg, 0); rc != C.VPX_CODEC_OK {
		return nil, fmt.Errorf("vp8: enc_config_default failed: %d", rc)
	}

	if fps <= 0 {
		fps = 30
	}
	cfg.g_w = C.uint(width)
	cfg.g_h = C.uint(height)
	cfg.g_timebase.num = 1
	cfg.g_timebase.den = C.int(fps)
	cfg.rc_target_bitrate = C.uint(bitrateKbps)
	cfg.g_pass = C.VPX_RC_ONE_PASS
	cfg.g_lag_in_frames = 0
	cfg.rc_end_usage = C.VPX_CBR
	cfg.g_error_resilient = C.VPX_ERROR_RESILIENT_DEFAULT
	cfg.rc_resize_allowed = 0
	cfg.kf_mode = C.VPX_KF_AUTO
	cfg.kf_max_dist = 999999 // effectively disables periodic keyframes; forced explicitly instead

	e := &Encoder{width: width, height: height}
	if rc := C.vp8_enc_init(&e.ctx, &cfg); rc != C.VPX_CODEC_OK {
		return nil, fmt.Errorf("vp8: enc_init failed: %d", rc)
	}
	if rc := C.vp8_set_cpuused(&e.ctx, 8); rc != C.VPX_CODEC_OK {
		C.vpx_codec_destroy(&e.ctx)
		return nil, fmt.Errorf("vp8: VP8E_SET_CPUUSED failed: %d", rc)
	}
	if img := C.vpx_img_alloc(&e.img, C.VPX_IMG_FMT_I420, C.uint(width), C.uint(height), 1); img == nil {
		C.vpx_codec_destroy(&e.ctx)
		return nil, errors.New("vp8: vpx_img_alloc failed")
	}

	return e, nil
}

// Encode encodes one packed I420 frame (Y plane w*h, then U and V
// planes at w/2*h/2 each). ptsUs is passed straight through to libvpx as
// the presentation timestamp; frame duration is always 1, matching the
// original. Returns the concatenated bytes of every VP8 bitstream packet
// libvpx emits for this frame (in practice always exactly one) — nil if
// libvpx produced no packet (e.g. an initial input frame under some
// rate-control configurations).
func (e *Encoder) Encode(yuv []byte, ptsUs int64, forceKeyframe bool) ([]byte, error) {
	w, h := e.width, e.height
	yLen := w * h
	uvW, uvH := w/2, h/2
	cLen := uvW * uvH
	if len(yuv) < yLen+2*cLen {
		return nil, fmt.Errorf("vp8: short buffer: got %d bytes, want >= %d", len(yuv), yLen+2*cLen)
	}

	copyPlane(e.img.planes[0], int(e.img.stride[0]), yuv[:yLen], w, h)
	copyPlane(e.img.planes[1], int(e.img.stride[1]), yuv[yLen:yLen+cLen], uvW, uvH)
	copyPlane(e.img.planes[2], int(e.img.stride[2]), yuv[yLen+cLen:yLen+2*cLen], uvW, uvH)

	var flags C.vpx_enc_frame_flags_t
	if forceKeyframe {
		flags = C.VPX_EFLAG_FORCE_KF
	}

	rc := C.vpx_codec_encode(&e.ctx, &e.img, C.vpx_codec_pts_t(ptsUs), 1, flags, C.VPX_DL_REALTIME)
	if rc != C.VPX_CODEC_OK {
		return nil, fmt.Errorf("vp8: encode failed: %d", rc)
	}

	var out []byte
	var iter C.vpx_codec_iter_t
	for {
		pkt := C.vpx_codec_get_cx_data(&e.ctx, &iter)
		if pkt == nil {
			break
		}
		if pkt.kind != C.VPX_CODEC_CX_FRAME_PKT {
			continue
		}
		var buf unsafe.Pointer
		var sz C.size_t
		C.vp8_frame_data(pkt, &buf, &sz)
		if sz > 0 {
			out = append(out, unsafe.Slice((*byte)(buf), int(sz))...)
		}
	}
	return out, nil
}

// Close releases the encoder's native resources. Safe to call once;
// calling it more than once is a programming error (double-free).
func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	C.vpx_img_free(&e.img)
	C.vpx_codec_destroy(&e.ctx)
	return nil
}

// copyPlane copies a tightly-packed w x h plane from src into dst,
// respecting dst's stride (which libvpx may pad beyond w for internal
// alignment).
func copyPlane(dst *C.uchar, stride int, src []byte, w, h int) {
	dstSlice := unsafe.Slice((*byte)(unsafe.Pointer(dst)), stride*h)
	for row := 0; row < h; row++ {
		copy(dstSlice[row*stride:row*stride+w], src[row*w:row*w+w])
	}
}
