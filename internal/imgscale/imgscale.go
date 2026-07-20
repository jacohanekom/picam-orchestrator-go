// Package imgscale downscales packed I420 (YUV420 planar) frames — used
// to cap the live web-displayed stream's resolution below the camera's
// native capture resolution without touching the capture itself
// (recording and snapshots stay full-res; only the WebRTC live encode
// path is downscaled).
package imgscale

// FitWithinMax returns the largest w x h (both forced even, preserving
// srcW:srcH aspect ratio) that fits within maxW x maxH, without ever
// upscaling — if src already fits, it's returned unchanged. A
// non-positive maxW or maxH means "no cap."
func FitWithinMax(srcW, srcH, maxW, maxH int) (w, h int) {
	if maxW <= 0 || maxH <= 0 || (srcW <= maxW && srcH <= maxH) {
		return srcW, srcH
	}
	scale := float64(maxW) / float64(srcW)
	if s := float64(maxH) / float64(srcH); s < scale {
		scale = s
	}
	w = int(float64(srcW) * scale)
	h = int(float64(srcH) * scale)
	w -= w % 2
	h -= h % 2
	if w < 2 {
		w = 2
	}
	if h < 2 {
		h = 2
	}
	return w, h
}

// DownscaleI420 box-filter (area-average) downscales a tightly-packed
// I420 buffer (Y plane srcW*srcH, then U and V at srcW/2*srcH/2 each)
// from srcW x srcH to dstW x dstH (both must be even). Returns src
// unchanged (not a copy) if the dimensions already match — callers that
// need to mutate the result afterward (e.g. burning in an OSD) should
// keep that in mind, same as any other frame buffer in this pipeline
// that's shared with the mailbox.
func DownscaleI420(src []byte, srcW, srcH, dstW, dstH int) []byte {
	if srcW == dstW && srcH == dstH {
		return src
	}

	srcYLen := srcW * srcH
	srcCLen := (srcW / 2) * (srcH / 2)
	dstYLen := dstW * dstH
	dstCLen := (dstW / 2) * (dstH / 2)

	dst := make([]byte, dstYLen+2*dstCLen)
	downscalePlane(src[:srcYLen], srcW, srcH, dst[:dstYLen], dstW, dstH)
	downscalePlane(src[srcYLen:srcYLen+srcCLen], srcW/2, srcH/2, dst[dstYLen:dstYLen+dstCLen], dstW/2, dstH/2)
	downscalePlane(src[srcYLen+srcCLen:srcYLen+2*srcCLen], srcW/2, srcH/2, dst[dstYLen+dstCLen:dstYLen+2*dstCLen], dstW/2, dstH/2)
	return dst
}

// downscalePlane area-averages one 8-bit plane from srcW x srcH into
// dst, sized dstW*dstH. Each output pixel is the mean of the
// (possibly multi-pixel, for a downscale) source region it covers —
// cheap area resampling, meaningfully less prone to aliasing than
// nearest-neighbor for the ~1.2x-2x downscale ratios this is actually
// used at (e.g. 2304x1296 -> 1920x1080).
func downscalePlane(src []byte, srcW, srcH int, dst []byte, dstW, dstH int) {
	scaleX := float64(srcW) / float64(dstW)
	scaleY := float64(srcH) / float64(dstH)

	for oy := 0; oy < dstH; oy++ {
		sy0 := int(float64(oy) * scaleY)
		sy1 := int(float64(oy+1) * scaleY)
		if sy1 <= sy0 {
			sy1 = sy0 + 1
		}
		if sy1 > srcH {
			sy1 = srcH
		}
		for ox := 0; ox < dstW; ox++ {
			sx0 := int(float64(ox) * scaleX)
			sx1 := int(float64(ox+1) * scaleX)
			if sx1 <= sx0 {
				sx1 = sx0 + 1
			}
			if sx1 > srcW {
				sx1 = srcW
			}
			sum, count := 0, 0
			for sy := sy0; sy < sy1; sy++ {
				row := sy * srcW
				for sx := sx0; sx < sx1; sx++ {
					sum += int(src[row+sx])
					count++
				}
			}
			dst[oy*dstW+ox] = byte(sum / count)
		}
	}
}
