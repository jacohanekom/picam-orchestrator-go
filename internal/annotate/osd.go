package annotate

import "time"

const osdPad = 4

func textWidth(s string) int { return len(s) * (kCW + 1) * kScale }
func textHeight() int        { return kCH * kScale }

func drawBg(y []byte, w, h, px, py, bw, bh int) {
	for yy := py; yy < py+bh && yy < h; yy++ {
		for x := px; x < px+bw && x < w; x++ {
			setY(y, w, h, x, yy, 16)
		}
	}
}

func drawString(y []byte, w, h, px, py int, text string) {
	cx := px
	for i := 0; i < len(text); i++ {
		drawChar(y, w, h, cx, py, text[i], 235, 16)
		cx += (kCW + 1) * kScale
	}
}

// DrawOSD burns "cam: <cameraLabel>" bottom-left (if showCameraID) and a
// UTC "YYYY-MM-DD HH:MM:SS" timestamp derived from timestampUs
// bottom-right (if showTime) into the Y-plane y. No-op if both flags are
// false.
func DrawOSD(y []byte, w, h int, timestampUs int64, cameraLabel string, showCameraID, showTime bool) {
	if !showCameraID && !showTime {
		return
	}
	th := textHeight()
	bottomY := h - th - osdPad

	if showCameraID {
		text := "cam: " + cameraLabel
		tw := textWidth(text)
		drawBg(y, w, h, osdPad-2, bottomY-2, tw+4, th+4)
		drawString(y, w, h, osdPad, bottomY, text)
	}

	if showTime {
		sec := timestampUs / 1_000_000
		text := time.Unix(sec, 0).UTC().Format("2006-01-02 15:04:05")
		tw := textWidth(text)
		x := w - tw - osdPad
		drawBg(y, w, h, x-2, bottomY-2, tw+4, th+4)
		drawString(y, w, h, x, bottomY, text)
	}
}
