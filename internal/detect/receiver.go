package detect

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"time"
)

// Run connects to picam-hailo's newline-delimited JSON detection stream
// at host:port, auto-reconnecting with a 2-second backoff on failure or
// disconnect, until ctx is cancelled. Every successfully parsed event is
// pushed into buf and, if non-nil, passed to onEvent.
func Run(ctx context.Context, host string, port int, buf *Buffer, onEvent func(Event)) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			if !sleepOrDone(ctx, 2*time.Second) {
				return
			}
			continue
		}
		log.Printf("[Detections] Connected to %s", addr)

		connDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				conn.Close()
			case <-connDone:
			}
		}()

		scanner := bufio.NewScanner(conn)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			var w wireEvent
			if err := json.Unmarshal(scanner.Bytes(), &w); err != nil {
				continue
			}
			evt := w.toEvent()
			if onEvent != nil {
				onEvent(evt)
			}
			buf.Push(evt)
		}
		close(connDone)
		conn.Close()

		if ctx.Err() != nil {
			return
		}
		log.Printf("[Detections] Disconnected, retrying...")
		if !sleepOrDone(ctx, 2*time.Second) {
			return
		}
	}
}

// sleepOrDone waits for d or ctx cancellation, returning false if ctx
// was cancelled first.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
