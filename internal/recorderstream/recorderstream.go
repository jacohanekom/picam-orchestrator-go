// Package recorderstream is a small client for picam-recorder's own
// always-live GET /stream endpoint (see that project's StreamServer):
// it connects, parses the multipart/x-mixed-replace response, and hands
// each JPEG frame to a callback -- reconnecting with a fixed backoff on
// any failure, for as long as the caller's context stays alive.
//
// This is what lets picam-orchestrator proxy picam-recorder's own
// main-quality JPEG compression for its live (non-annotated, no-OSD)
// main view instead of separately re-compressing the exact same frames
// itself -- see cmd/picam-orchestrator's runMainLoop.
package recorderstream

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"time"
)

// client has no overall Timeout -- this connection is meant to stay open
// indefinitely -- but does bound the initial TCP connect, mirroring
// picam-frontend-go's backendhttp.Client.httpStream field for the exact
// same reason.
var client = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	},
}

// Run connects to picam-recorder's GET /stream?stream=<tier> endpoint on
// host:port and calls onFrame for every JPEG frame received, reconnecting
// after a fixed backoff on any failure. Blocks until ctx is cancelled.
func Run(ctx context.Context, host string, port int, tier string, onFrame func([]byte)) {
	for ctx.Err() == nil {
		if err := stream(ctx, host, port, tier, onFrame); err != nil && ctx.Err() == nil {
			log.Printf("[RecorderStream] %s: %v -- retrying in 3s", tier, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func stream(ctx context.Context, host string, port int, tier string, onFrame func([]byte)) error {
	url := fmt.Sprintf("http://%s:%d/stream?stream=%s", host, port, tier)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || params["boundary"] == "" {
		return fmt.Errorf("response has no multipart boundary")
	}

	mr := multipart.NewReader(resp.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err != nil {
			return err
		}
		jpg, err := io.ReadAll(part)
		part.Close()
		if err != nil {
			return err
		}
		onFrame(jpg)
	}
}
