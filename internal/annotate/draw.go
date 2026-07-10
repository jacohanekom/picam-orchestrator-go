// Package annotate draws bounding boxes/labels and an OSD (camera ID +
// timestamp) directly into a YUV420 frame's Y (luma) plane — no chroma
// is touched, since box/text contrast only needs luma.
package annotate

import (
	"fmt"

	"picam-orchestrator/internal/detect"
)

func setY(y []byte, w, h, x, yy int, v byte) {
	if x >= 0 && x < w && yy >= 0 && yy < h {
		y[yy*w+x] = v
	}
}

func hline(y []byte, w, h, x0, x1, yy int, v byte) {
	for k := 0; k < 2; k++ {
		for x := x0; x <= x1; x++ {
			setY(y, w, h, x, yy+k, v)
		}
	}
}

func vline(y []byte, w, h, x, y0, y1 int, v byte) {
	for k := 0; k < 2; k++ {
		for yy := y0; yy <= y1; yy++ {
			setY(y, w, h, x+k, yy, v)
		}
	}
}

func rect(y []byte, w, h, x0, y0, x1, y1 int, v byte) {
	hline(y, w, h, x0, x1, y0, v)
	hline(y, w, h, x0, x1, y1-1, v)
	vline(y, w, h, x0, y0, y1, v)
	vline(y, w, h, x1-1, y0, y1, v)
}

func drawChar(y []byte, w, h, px, py int, ch byte, fg, bg byte) {
	if ch >= 'a' && ch <= 'z' {
		ch -= 32
	}
	idx := int(ch) - 0x20
	if idx < 0 || idx >= len(font5x7) {
		idx = 0
	}
	g := font5x7[idx]
	for col := 0; col < kCW; col++ {
		for row := 0; row < kCH; row++ {
			lit := (g[col]>>uint(row))&1 != 0
			v := bg
			if lit {
				v = fg
			}
			for sy := 0; sy < kScale; sy++ {
				for sx := 0; sx < kScale; sx++ {
					setY(y, w, h, px+col*kScale+sx, py+row*kScale+sy, v)
				}
			}
		}
	}
}

func drawLabel(y []byte, w, h, x0, y0 int, text string) {
	tw := len(text) * (kCW + 1) * kScale
	th := kCH*kScale + 4
	ly := y0 - th
	if ly < 0 {
		ly = y0 + 2
	}
	for yy := ly; yy < ly+th && yy < h; yy++ {
		for x := x0; x < x0+tw+4 && x < w; x++ {
			setY(y, w, h, x, yy, 16)
		}
	}
	cx := x0 + 2
	for i := 0; i < len(text); i++ {
		drawChar(y, w, h, cx, ly+2, text[i], 235, 16)
		cx += (kCW + 1) * kScale
	}
}

// DrawDetections draws each detection's bounding box and a
// "<class> <conf%>" label into the Y-plane y (width w, height h).
// Detection coordinates are normalized [0,1]; they're scaled to this
// frame's own dimensions.
func DrawDetections(y []byte, w, h int, dets []detect.Detection) {
	for _, d := range dets {
		x0 := int(d.X0 * float32(w))
		y0 := int(d.Y0 * float32(h))
		x1 := int(d.X1 * float32(w))
		y1 := int(d.Y1 * float32(h))
		rect(y, w, h, x0, y0, x1, y1, 235)
		label := fmt.Sprintf("%s %.0f%%", d.Class, d.Conf*100)
		drawLabel(y, w, h, x0, y0, label)
	}
}
