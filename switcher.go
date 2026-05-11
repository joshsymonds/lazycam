package main

import (
	"context"
	"errors"
	"log/slog"
)

// Switcher is the abstraction the daemon's event loop calls on
// transitions. The dry-run implementation logs the intended action;
// the live implementation (task #5) issues a SetCurrentProgramScene
// RPC over OBS WebSocket v5.
type Switcher interface {
	SetScene(ctx context.Context, name string) error
	Close() error
}

// switcherOptions carries everything the constructor needs across
// modes. dryRun gates the implementation; the remaining fields are
// consumed only by the live switcher (task #5) but defined here so the
// constructor signature is stable.
type switcherOptions struct {
	dryRun       bool
	obsURL       string
	sceneActive  string
	sceneStandby string
}

// errLiveNotImplemented is returned by newSwitcher when called in live
// mode (dryRun=false) before task #5 lands the OBS WebSocket client.
// Removed in task #5.
var errLiveNotImplemented = errors.New("live OBS WebSocket client not yet implemented (task #5)")

// newSwitcher returns the appropriate Switcher for the requested mode.
// Live mode currently returns errLiveNotImplemented.
func newSwitcher(logger *slog.Logger, opts switcherOptions) (Switcher, error) {
	if opts.dryRun {
		return &dryRunSwitcher{logger: logger}, nil
	}
	return nil, errLiveNotImplemented
}

// dryRunSwitcher logs the intended scene transition without contacting
// OBS. Useful for piping the daemon through CI / smoke tests / config
// validation without a live OBS instance.
type dryRunSwitcher struct {
	logger *slog.Logger
}

// SetScene logs the request and returns nil. The context is accepted
// for signature parity with the live switcher (which will respect
// ctx for timeout/cancellation).
func (d *dryRunSwitcher) SetScene(ctx context.Context, name string) error {
	d.logger.InfoContext(ctx, "would set scene", "scene", name)
	return nil
}

// Close is a no-op for the dry-run mode.
func (d *dryRunSwitcher) Close() error {
	return nil
}
