//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/tailscale/ipn"
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

func TestTailscaleWaitReadyBlocksUntilRunningWithIP(t *testing.T) {
	var hasIP atomic.Bool
	proxy := &Tailscale{
		readyCheck: hasIP.Load,
	}
	starting := ipn.Starting
	proxy.updateBackendState(&starting, false)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- proxy.waitReady(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("waitReady returned early while Starting: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	running := ipn.Running
	proxy.updateBackendState(&running, false)
	select {
	case err := <-done:
		t.Fatalf("waitReady returned before Tailscale IP arrived: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	hasIP.Store(true)
	proxy.updateBackendState(nil, true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitReady failed once Running with IP: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitReady did not wake after Tailscale IP arrived")
	}
}

func TestTailscaleWaitReadyFailsFastOnPausedOrAuthStates(t *testing.T) {
	pausedProxy := &Tailscale{}
	pausedProxy.paused.Store(true)
	if err := pausedProxy.waitReady(context.Background()); err == nil {
		t.Fatal("waitReady accepted paused outbound")
	}

	for _, state := range []ipn.State{ipn.NeedsLogin, ipn.NeedsMachineAuth} {
		proxy := &Tailscale{}
		s := state
		proxy.updateBackendState(&s, false)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := proxy.waitReady(ctx)
		cancel()
		if err == nil {
			t.Fatalf("waitReady did not fail fast for %v", state)
		}
	}
}

func TestTailscaleSettleRunningWaitsForIPAndIgnoresTimeout(t *testing.T) {
	prevTimeout := tailscaleStartSettleTimeout
	tailscaleStartSettleTimeout = 40 * time.Millisecond
	t.Cleanup(func() { tailscaleStartSettleTimeout = prevTimeout })

	timedOutProxy := &Tailscale{}
	starting := ipn.Starting
	timedOutProxy.updateBackendState(&starting, false)
	timedOutProxy.settleRunning(context.Background())

	tailscaleStartSettleTimeout = time.Second
	var hasIP atomic.Bool
	readyProxy := &Tailscale{readyCheck: hasIP.Load}
	readyProxy.updateBackendState(&starting, false)

	done := make(chan struct{})
	go func() {
		readyProxy.settleRunning(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("settleRunning returned before Running with IP")
	case <-time.After(25 * time.Millisecond):
	}

	hasIP.Store(true)
	running := ipn.Running
	readyProxy.updateBackendState(&running, true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("settleRunning did not finish once Running with IP")
	}

	loginProxy := &Tailscale{}
	needsLogin := ipn.NeedsLogin
	loginProxy.updateBackendState(&needsLogin, false)
	loginDone := make(chan struct{})
	go func() {
		loginProxy.settleRunning(context.Background())
		close(loginDone)
	}()
	select {
	case <-loginDone:
	case <-time.After(time.Second):
		t.Fatal("settleRunning did not exit early on NeedsLogin")
	}
}

func TestTailscaleSettleStoppedWaitsUntilStopped(t *testing.T) {
	prevTimeout := tailscaleStopSettleTimeout
	tailscaleStopSettleTimeout = time.Second
	t.Cleanup(func() { tailscaleStopSettleTimeout = prevTimeout })

	proxy := &Tailscale{}
	proxy.paused.Store(true)
	running := ipn.Running
	proxy.updateBackendState(&running, false)

	done := make(chan struct{})
	go func() {
		proxy.settleStopped(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("settleStopped returned before state became Stopped")
	case <-time.After(25 * time.Millisecond):
	}

	stopped := ipn.Stopped
	proxy.updateBackendState(&stopped, false)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("settleStopped did not wake on Stopped")
	}
}
