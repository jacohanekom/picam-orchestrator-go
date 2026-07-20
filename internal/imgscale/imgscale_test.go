package imgscale

import "testing"

func TestFitWithinMax(t *testing.T) {
	cases := []struct {
		name                   string
		srcW, srcH             int
		maxW, maxH             int
		wantW, wantH           int
	}{
		{"already fits, unchanged", 640, 360, 1920, 1080, 640, 360},
		{"exact fit, unchanged", 1920, 1080, 1920, 1080, 1920, 1080},
		{"downscale, same aspect ratio", 2304, 1296, 1920, 1080, 1920, 1080},
		{"never upscale", 320, 180, 1920, 1080, 320, 180},
		{"no cap configured", 2304, 1296, 0, 0, 2304, 1296},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, h := FitWithinMax(c.srcW, c.srcH, c.maxW, c.maxH)
			if w != c.wantW || h != c.wantH {
				t.Errorf("FitWithinMax(%d,%d,%d,%d) = %d,%d; want %d,%d",
					c.srcW, c.srcH, c.maxW, c.maxH, w, h, c.wantW, c.wantH)
			}
			if w%2 != 0 || h%2 != 0 {
				t.Errorf("FitWithinMax(%d,%d,%d,%d) = %d,%d; must be even for YUV420", c.srcW, c.srcH, c.maxW, c.maxH, w, h)
			}
		})
	}
}

func TestDownscaleI420_UniformColor(t *testing.T) {
	const srcW, srcH = 2304, 1296
	const dstW, dstH = 1920, 1080

	yLen := srcW * srcH
	cLen := (srcW / 2) * (srcH / 2)
	src := make([]byte, yLen+2*cLen)
	for i := 0; i < yLen; i++ {
		src[i] = 128
	}
	for i := yLen; i < yLen+2*cLen; i++ {
		src[i] = 200
	}

	dst := DownscaleI420(src, srcW, srcH, dstW, dstH)

	wantYLen := dstW * dstH
	wantCLen := (dstW / 2) * (dstH / 2)
	if len(dst) != wantYLen+2*wantCLen {
		t.Fatalf("len(dst) = %d, want %d", len(dst), wantYLen+2*wantCLen)
	}
	for i := 0; i < wantYLen; i++ {
		if dst[i] != 128 {
			t.Fatalf("Y[%d] = %d, want 128 (uniform input should downscale to the same uniform value)", i, dst[i])
		}
	}
	for i := wantYLen; i < len(dst); i++ {
		if dst[i] != 200 {
			t.Fatalf("chroma[%d] = %d, want 200", i, dst[i])
		}
	}
}

func TestDownscaleI420_NoOpWhenSameSize(t *testing.T) {
	const w, h = 640, 360
	yLen := w * h
	cLen := (w / 2) * (h / 2)
	src := make([]byte, yLen+2*cLen)
	for i := range src {
		src[i] = byte(i % 256)
	}
	dst := DownscaleI420(src, w, h, w, h)
	if &dst[0] != &src[0] {
		t.Errorf("DownscaleI420 with matching dimensions should return src unchanged, not a copy")
	}
}
