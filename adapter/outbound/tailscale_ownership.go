//go:build with_gvisor && !no_tailscale

package outbound

import (
	"errors"
	"fmt"
	"sync"
)

type tailscaleOwnership struct {
	sync.Mutex
	nextGeneration uint64
	lastStarted    uint64
	active         *Tailscale
}

var tailscaleOwners sync.Map

func reserveTailscaleOwnership(stateDir string) (*tailscaleOwnership, uint64) {
	value, _ := tailscaleOwners.LoadOrStore(stateDir, &tailscaleOwnership{})
	owner := value.(*tailscaleOwnership)
	owner.Lock()
	defer owner.Unlock()
	owner.nextGeneration++
	return owner, owner.nextGeneration
}

func (t *Tailscale) lockControl() func() {
	if t.ownership != nil {
		t.ownership.Lock()
	}
	t.controlMu.Lock()
	return func() {
		t.controlMu.Unlock()
		if t.ownership != nil {
			t.ownership.Unlock()
		}
	}
}

func (t *Tailscale) checkOwnershipLocked() error {
	if t.closed || (t.ownership != nil && t.generation < t.ownership.lastStarted) {
		return errors.New("Tailscale outbound was closed or replaced")
	}
	return nil
}

func (t *Tailscale) claimOwnershipLocked() error {
	if err := t.checkOwnershipLocked(); err != nil {
		return err
	}
	owner := t.ownership
	if owner == nil || owner.active == t {
		return nil
	}
	if previous := owner.active; previous != nil {
		// Config reloads retain old adapters for live connections; tsnet identities cannot overlap.
		previous.controlMu.Lock()
		err := previous.closeLocked()
		previous.controlMu.Unlock()
		if err != nil {
			return fmt.Errorf("close previous Tailscale instance: %w", err)
		}
	}
	owner.active = t
	owner.lastStarted = t.generation
	return nil
}
