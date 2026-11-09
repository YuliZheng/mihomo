//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/tailscale/ipn"
)

func (t *Tailscale) pausePath() string {
	return filepath.Join(t.option.StateDir, "flclash.paused")
}

func (t *Tailscale) savePaused(paused bool) error {
	if !paused {
		err := os.Remove(t.pausePath())
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.MkdirAll(t.option.StateDir, 0700); err != nil {
		return err
	}
	return os.WriteFile(t.pausePath(), nil, 0600)
}

func SetTailscaleRunning(ctx context.Context, proxy C.ProxyAdapter, running bool) error {
	switch adapter := proxy.(type) {
	case *Tailscale:
		return adapter.setRunning(ctx, running)
	case *autoCloseProxyAdapter:
		return SetTailscaleRunning(ctx, adapter.ProxyAdapter, running)
	default:
		return fmt.Errorf("proxy %q is not a Tailscale outbound", proxy.Name())
	}
}

func (t *Tailscale) setRunning(ctx context.Context, running bool) (err error) {
	t.controlMu.Lock()
	defer t.controlMu.Unlock()
	previous := t.paused.Load()
	if err = t.savePaused(!running); err != nil {
		return err
	}
	t.paused.Store(!running)
	defer func() {
		if err != nil {
			t.paused.Store(previous)
			err = errors.Join(err, t.savePaused(previous))
			if t.serverStarted.Load() {
				restoreCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = errors.Join(err, t.editRunning(restoreCtx, !previous))
			}
		}
	}()
	if running {
		if err = t.startLocked(); err != nil {
			return err
		}
		if err = t.waitBackendInitialized(ctx); err != nil {
			return err
		}
	} else if !t.serverStarted.Load() {
		return nil
	}
	return t.editRunning(ctx, running)
}

func (t *Tailscale) editRunning(ctx context.Context, running bool) error {
	client, err := t.server.LocalClient()
	if err != nil {
		return err
	}
	_, err = client.EditPrefs(ctx, &ipn.MaskedPrefs{
		Prefs:          ipn.Prefs{WantRunning: running},
		WantRunningSet: true,
	})
	return err
}
