// Package rawframe reassembles the chunked UDP YUV420 frame protocol
// spoken by picam-raw and maintains a "live" latest-frame mailbox per
// resolution for the zero-delay streaming path.
package rawframe

import (
	"encoding/binary"
	"time"
)

const (
	headerSize       = 8  // frameSeq(4) + chunkSeq(2) + totalChunks(2)
	chunk0HeaderSize = 32 // headerSize + timestampUs(8) + cameraIndex(1) + label(15, unused/skipped)
)

// chunkHeader is the per-datagram header picam-raw prepends to every UDP
// chunk. The extended fields (timestampUs, cameraIndex) are only present,
// and only valid, on chunkSeq 0 of a frame. picam-raw also embeds a
// 15-byte camera label in that same extended header, but the upstream
// pipeline never actually consumes it (camera display labels come from
// this process's own config instead — see Config.CameraLabel), so it's
// intentionally not parsed here.
type chunkHeader struct {
	frameSeq    uint32
	chunkSeq    uint16
	totalChunks uint16
	timestampUs int64
	cameraIndex uint8
}

// parseHeader decodes buf's header. Callers must ensure len(buf) >= headerSize;
// the extended chunk-0 fields are only populated when chunkSeq==0 and
// len(buf) >= chunk0HeaderSize.
func parseHeader(buf []byte) chunkHeader {
	h := chunkHeader{
		frameSeq:    binary.BigEndian.Uint32(buf[0:4]),
		chunkSeq:    binary.BigEndian.Uint16(buf[4:6]),
		totalChunks: binary.BigEndian.Uint16(buf[6:8]),
	}
	if h.chunkSeq == 0 && len(buf) >= chunk0HeaderSize {
		h.timestampUs = int64(binary.BigEndian.Uint64(buf[8:16]))
		h.cameraIndex = buf[16]
	}
	return h
}

// RawFrame is one fully reassembled YUV420 (I420) frame.
type RawFrame struct {
	Data        []byte
	Width       int
	Height      int
	TimestampUs int64
	CameraIndex uint8
	Arrival     time.Time
}
