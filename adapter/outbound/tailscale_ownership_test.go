//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func ownershipTestProxy(t *testing.T, dir string) *Tailscale {
	t.Helper()
	proxy, err := NewTailscale(TailscaleOption{Name: "TAILNET", StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	return proxy
}

func activateTestOwnership(proxy *Tailscale) error {
	unlock := proxy.lockControl()
	defer unlock()
	return proxy.claimOwnershipLocked()
}

func TestTailscaleReloadRetiresPreviousIdentity(t *testing.T) {
	previousHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previousHome) })
	old := ownershipTestProxy(t, "tailscale")
	if err := activateTestOwnership(old); err != nil {
		t.Fatal(err)
	}
	replacement := ownershipTestProxy(t, filepath.Join(C.Path.HomeDir(), "tailscale"))
	if old.ctx.Err() != nil {
		t.Fatal("parsing a replacement stopped the active identity before activation")
	}
	if err := activateTestOwnership(replacement); err != nil {
		t.Fatal(err)
	}
	if old.ctx.Err() == nil || replacement.ctx.Err() != nil {
		t.Fatal("the previous identity must stop before its replacement becomes active")
	}
	if err := SetTailscaleRunning(context.Background(), old, false); err == nil {
		t.Fatal("a stale control request changed the replacement's persisted running intent")
	}
	if _, err := os.Stat(replacement.pausePath()); !os.IsNotExist(err) {
		t.Fatalf("stale pause wrote the shared pause marker: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if replacement.ownership.active != replacement {
		t.Fatal("late cleanup of an old adapter released the replacement")
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := activateTestOwnership(old); err == nil {
		t.Fatal("an obsolete adapter reclaimed a retired node identity")
	}
}

func TestTailscaleConcurrentReloadKeepsNewestIdentity(t *testing.T) {
	previousHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previousHome) })
	proxies := make([]*Tailscale, 12)
	for i := range proxies {
		proxies[i] = ownershipTestProxy(t, "tailscale")
	}
	var wg sync.WaitGroup
	for _, proxy := range proxies {
		wg.Add(1)
		go func(p *Tailscale) {
			defer wg.Done()
			_ = activateTestOwnership(p)
		}(proxy)
	}
	wg.Wait()
	newest := proxies[len(proxies)-1]
	if newest.ownership.active != newest {
		t.Fatal("a late request from an older config replaced the newest identity")
	}
	for _, proxy := range proxies[:len(proxies)-1] {
		if err := activateTestOwnership(proxy); err == nil {
			t.Fatal("an older identity can still activate")
		}
	}
	other := ownershipTestProxy(t, "other-tailnet")
	if err := activateTestOwnership(other); err != nil || newest.ctx.Err() != nil {
		t.Fatalf("a different state directory interrupted the current identity: %v", err)
	}
}
