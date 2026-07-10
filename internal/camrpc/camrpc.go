// Package camrpc sends the one-shot camera-switch command to picam-raw's
// plain-text CommandServer.
package camrpc

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"time"
)

// SwitchCamera connects to host:port (picam-raw's CommandServer),
// sends "switch<cameraID>\n", and returns picam-raw's single-line JSON
// response verbatim (trailing newline trimmed).
//
// reached is true whenever the TCP round-trip completed at all — even
// if picam-raw's own response body says {"ok":false,...} for a bad
// index, that still counts as "reached" from this process's point of
// view. reached is false only on a connect/send/receive failure, in
// which case response holds a synthetic error JSON string instead.
func SwitchCamera(host string, port, cameraID int) (reached bool, response string) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return false, `{"ok":false,"error":"connect failed"}`
	}
	defer conn.Close()

	cmd := "switch" + strconv.Itoa(cameraID) + "\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return false, `{"ok":false,"error":"send failed"}`
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && line == "" {
		return false, `{"ok":false,"error":"no response from picam-raw"}`
	}
	return true, strings.TrimRight(line, "\r\n")
}
