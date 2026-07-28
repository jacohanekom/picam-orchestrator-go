// Package recorder drives picam-recorder's plain-text TCP control
// protocol and orchestrates detection-triggered recording sessions.
package recorder

import (
	"net"
	"strconv"
	"strings"
	"time"
)

// sendCommand connects to host:port, sends cmd+"\n", and reads the
// response until a blank line ("\n\n"), EOF, or a 5-second read timeout
// — whichever comes first. Returns "" on a connect/send failure.
func sendCommand(host string, port int, cmd string) string {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()

	if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
		return ""
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var resp strings.Builder
	buf := make([]byte, 512)
	for !strings.Contains(resp.String(), "\n\n") {
		n, err := conn.Read(buf)
		if n > 0 {
			resp.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return resp.String()
}

// Start tells picam-recorder to begin recording under name and returns
// the file path it reports ("file=<path>" in its response), or "" on
// any failure.
func Start(host string, port int, name string) string {
	resp := sendCommand(host, port, "start "+name)
	idx := strings.Index(resp, "file=")
	if idx < 0 {
		return ""
	}
	rest := resp[idx+len("file="):]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		rest = rest[:nl]
	}
	return strings.TrimRight(rest, "\r")
}

// Stop tells picam-recorder to stop the current recording.
func Stop(host string, port int) {
	sendCommand(host, port, "stop")
}
