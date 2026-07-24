package discovery

import (
	"os"
	"testing"
)

func TestResolveName(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Skip("os.Hostname unavailable in this environment")
	}

	cases := []struct {
		name       string
		configured string
		want       string
	}{
		{"configured value wins", "front", "front"},
		{"blank falls back to hostname", "", hostname},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveName(tc.configured); got != tc.want {
				t.Errorf("ResolveName(%q) = %q, want %q", tc.configured, got, tc.want)
			}
		})
	}
}

func TestResolveLabel(t *testing.T) {
	cases := []struct {
		name       string
		configured string
		resolved   string
		want       string
	}{
		{"configured value wins", "Front Yard", "front", "Front Yard"},
		{"blank falls back to name", "", "front", "front"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveLabel(tc.configured, tc.resolved); got != tc.want {
				t.Errorf("ResolveLabel(%q, %q) = %q, want %q", tc.configured, tc.resolved, got, tc.want)
			}
		})
	}
}

func TestAdvertiseDisabled(t *testing.T) {
	srv, err := Advertise(Config{Enabled: false})
	if err != nil {
		t.Fatalf("Advertise(disabled) returned error: %v", err)
	}
	if srv != nil {
		t.Fatalf("Advertise(disabled) returned a non-nil server")
	}
}
