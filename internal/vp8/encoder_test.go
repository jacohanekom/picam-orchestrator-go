package vp8

import "testing"

func TestEncodeKeyframe(t *testing.T) {
	const w, h = 64, 48
	enc, err := NewEncoder(w, h, 500, 15)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	defer enc.Close()

	yuv := make([]byte, w*h*3/2)
	for i := range yuv {
		yuv[i] = 128
	}

	out, err := enc.Encode(yuv, 0, true)
	if err != nil {
		t.Fatalf("Encode (forced keyframe): %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("Encode returned no bytes for a forced keyframe")
	}
	// VP8 keyframe payload always starts with a 3-byte uncompressed
	// tag whose low bit (frame_type) is 0 for a keyframe.
	if out[0]&0x01 != 0 {
		t.Errorf("expected keyframe tag (low bit 0), got first byte 0x%02x", out[0])
	}

	// A second, non-keyframe encode at a later pts should also succeed.
	for i := range yuv {
		yuv[i] = 130
	}
	out2, err := enc.Encode(yuv, 66667, false)
	if err != nil {
		t.Fatalf("Encode (delta frame): %v", err)
	}
	if len(out2) == 0 {
		t.Fatalf("Encode returned no bytes for a delta frame")
	}
}
