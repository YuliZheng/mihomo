//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestTailscalePauseSurvivesReloadAndBlocksTraffic(t *testing.T) {
	previousHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previousHome) })
	option := TailscaleOption{Name: "test-tailnet", StateDir: "tailscale"}
	proxy, err := NewTailscale(option)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetTailscaleRunning(context.Background(), proxy, false); err != nil {
		t.Fatal(err)
	}
	if proxy.serverStarted.Load() {
		t.Fatal("pausing an unused outbound must not start Tailscale")
	}
	_ = proxy.Close()
	proxy, err = NewTailscale(option)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	status, err := GetTailscaleStatus(context.Background(), proxy, true, false)
	if err != nil || status.BackendState != "Stopped" {
		t.Fatalf("reloaded paused status: %+v, %v", status, err)
	}
	if _, err := proxy.DialContext(context.Background(), &C.Metadata{}); err == nil {
		t.Fatal("paused TCP must be blocked")
	}
	if _, err := proxy.ListenPacketContext(context.Background(), &C.Metadata{}); err == nil {
		t.Fatal("paused UDP must be blocked")
	}
	if proxy.serverStarted.Load() {
		t.Fatal("traffic must not reactivate a paused outbound")
	}
	if err := proxy.savePaused(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(proxy.pausePath()); !os.IsNotExist(err) {
		t.Fatalf("resume must remove the pause marker: %v", err)
	}
}

func TestTailscalePauseStorageFailureKeepsPreviousState(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(stateDir, nil, 0600); err != nil {
		t.Fatal(err)
	}
	proxy := &Tailscale{option: TailscaleOption{StateDir: stateDir}}
	if err := SetTailscaleRunning(context.Background(), proxy, false); err == nil {
		t.Fatal("storage failure must reach the caller")
	}
	if proxy.paused.Load() {
		t.Fatal("failed pause must not change the displayed state")
	}
}
