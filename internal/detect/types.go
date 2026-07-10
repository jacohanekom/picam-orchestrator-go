// Package detect handles picam-hailo's newline-delimited JSON detection
// stream: parsing, a timestamp-indexed recent-event buffer, and an
// auto-reconnecting TCP receiver.
package detect

// Detection is one object detection, with box coordinates normalized to
// [0,1] relative to the model's input frame (not pixel coords, and not
// specific to main vs. lores resolution — the same normalized box is
// used to annotate either resolution's frame).
type Detection struct {
	Class string
	Conf  float32
	X0    float32
	Y0    float32
	X1    float32
	Y1    float32
}

// Event is one line of picam-hailo's detection stream.
type Event struct {
	TsUs       int64
	FrameSeq   uint32
	Detections []Detection
}

// wireEvent mirrors picam-hailo's actual JSON wire shape, including the
// nested "box" object.
type wireEvent struct {
	TsUs       int64  `json:"ts_us"`
	FrameSeq   uint32 `json:"frame_seq"`
	Detections []struct {
		Class string  `json:"class"`
		Conf  float32 `json:"conf"`
		Box   struct {
			X0 float32 `json:"x0"`
			Y0 float32 `json:"y0"`
			X1 float32 `json:"x1"`
			Y1 float32 `json:"y1"`
		} `json:"box"`
	} `json:"detections"`
}

func (w wireEvent) toEvent() Event {
	e := Event{TsUs: w.TsUs, FrameSeq: w.FrameSeq}
	if len(w.Detections) > 0 {
		e.Detections = make([]Detection, len(w.Detections))
		for i, d := range w.Detections {
			e.Detections[i] = Detection{
				Class: d.Class,
				Conf:  d.Conf,
				X0:    d.Box.X0,
				Y0:    d.Box.Y0,
				X1:    d.Box.X1,
				Y1:    d.Box.Y1,
			}
		}
	}
	return e
}
