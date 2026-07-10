// Package statussrv implements the plain-text TCP status protocol
// ("echo status | nc host port"): any connection that sends any line of
// text (content ignored) gets back a fixed key=value block and is then
// closed.
package statussrv

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"

	"picam-orchestrator/internal/pipestat"
)

// Run listens on port and serves the status protocol until ctx is
// cancelled. Unlike the main HTTP control server, a bind failure here is
// logged but non-fatal to the process, matching the C++ original's
// asymmetry between the two servers.
func Run(ctx context.Context, port int, status *pipestat.Status) {
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[Status] FATAL: bind() failed on port %d: %v", port, err)
		return
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed -> shutdown
		}
		go handleConn(conn, status)
	}
}

func handleConn(conn net.Conn, status *pipestat.Status) {
	defer conn.Close()
	// The request line's content is never inspected — any input
	// (including EOF) is enough to trigger a response, matching the
	// original's "read until newline, ignore what it says" behavior.
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')

	snap := status.Snapshot()
	fmt.Fprintf(conn,
		"ok=true\nframes_in=%d\nframes_out=%d\nmatched=%d\nfps=0.0\ndelay_buffer_depth=%d\nclients=%d\n\n",
		snap.FramesIn, snap.FramesOut, snap.Matched, snap.DelayBufferDepth, snap.Clients)
}
