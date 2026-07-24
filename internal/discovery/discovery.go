// Package discovery advertises this picam-orchestrator instance over
// mDNS/DNS-SD (Zeroconf/Bonjour) so picam-frontend can find it on the LAN
// automatically, with no static host list to maintain by hand. See the
// sibling picam-frontend-go's own internal/discovery package for the
// browsing side — ServiceType here is the single source of truth both
// sides must agree on.
package discovery

import (
	"fmt"
	"os"

	"github.com/libp2p/zeroconf/v2"
)

// ServiceType is the DNS-SD service type this process registers under.
// picam-frontend-go hardcodes the identical literal on its browsing side
// rather than reading it from here (the two are separate Go modules) —
// keep them in sync by hand if this ever changes.
const ServiceType = "_picam-orchestrator._tcp"

// Config is the subset of the process config Advertise needs.
type Config struct {
	Enabled  bool
	Name     string // short id used in picam-frontend's ?pi=NAME URLs; blank = OS hostname
	Label    string // display label shown in picam-frontend's UI; blank = same as Name
	HTTPPort int
}

// ResolveName returns configured, or the OS hostname if configured is
// blank. Pulled out as a pure function so the fallback is unit-testable
// without touching the network.
func ResolveName(configured string) string {
	if configured != "" {
		return configured
	}
	host, err := os.Hostname()
	if err != nil {
		return "picam-orchestrator"
	}
	return host
}

// ResolveLabel returns configured, or name if configured is blank.
func ResolveLabel(configured, name string) string {
	if configured != "" {
		return configured
	}
	return name
}

// Advertise registers this process as a picam-orchestrator instance on
// the local network. Returns (nil, nil) if cfg.Enabled is false. The
// returned *zeroconf.Server should be Shutdown() during graceful
// process shutdown so it sends mDNS goodbye packets instead of leaving a
// stale entry on the LAN until TTL expiry.
func Advertise(cfg Config) (*zeroconf.Server, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	name := ResolveName(cfg.Name)
	label := ResolveLabel(cfg.Label, name)

	srv, err := zeroconf.Register(name, ServiceType, "local.", cfg.HTTPPort, []string{"label=" + label}, nil)
	if err != nil {
		return nil, fmt.Errorf("discovery: register %s: %w", name, err)
	}
	return srv, nil
}
