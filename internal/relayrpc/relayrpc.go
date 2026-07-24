// Package relayrpc sends one-shot on/off commands to pi-relay-control's
// plain-text TCP command server.
package relayrpc

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"time"
)

// SetRelay connects to host:port (pi-relay-control's command server),
// sends "on\n" or "off\n", and returns pi-relay-control's single-line
// response verbatim (trailing newline trimmed).
//
// reached is true whenever the TCP round-trip completed at all, same
// contract as internal/camrpc.SwitchCamera — reached is false only on
// a connect/send/receive failure, in which case response holds a
// synthetic error string instead.
func SetRelay(host string, port int, on bool) (reached bool, response string) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return false, "ERR connect failed"
	}
	defer conn.Close()

	cmd := "off\n"
	if on {
		cmd = "on\n"
	}
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return false, "ERR send failed"
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && line == "" {
		return false, "ERR no response from pi-relay-control"
	}
	return true, strings.TrimRight(line, "\r\n")
}
