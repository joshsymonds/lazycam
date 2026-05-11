# lazycam

On-demand v4l2loopback producer gating, for OBS virtual-cam privacy.

## What it does

When OBS feeds a v4l2loopback device (typically `/dev/video10`) so other
apps can use it as a webcam, OBS holds the real camera open continuously
— which means the hardware privacy LED stays on even when nothing is
actually using the virtual cam. `lazycam` closes that gap: it watches
the loopback device with inotify and asks OBS (via WebSocket) to switch
to a "Standby" scene when no consumer is attached, and back to "Active"
when one shows up. OBS releases the real camera's file descriptor on the
scene flip → LED genuinely off until something needs it.

Inotify on a character device for open/close events is the same trick
[hajifkd/virtcam](https://github.com/hajifkd/virtcam) uses; lazycam keeps
OBS in the pipeline so the user's filter chain (face tracking,
background removal, etc.) keeps working.

## Status

V1 functional. The daemon:

- Watches `/dev/video10` with inotify for open / close events
- Maintains an in-process consumer ref-count and emits stable
  Activate / Deactivate transitions on 0↔N boundaries
- Sends `SetCurrentProgramScene` to OBS via WebSocket v5 (via
  [andreykaipov/goobs](https://github.com/andreykaipov/goobs)) on each
  transition, reconnecting with exponential backoff on disconnect
- Publishes newline-delimited JSON state events on a per-user UNIX
  socket (`$XDG_RUNTIME_DIR/lazycam.sock`) for external indicators
- Ships a `homeManagerModules.default` flake output declaring the
  `systemd --user` service, and a `quickshellModules.lazycam-indicator`
  output for Quickshell-based bar widgets

Open work: the daemon does not yet bootstrap its ref-count from a
`/proc/*/fd/` scan at startup, so a daemon-restart-while-consumers-attached
could miss the initial open event (the ref-count clamps at zero on the
later close — defensive, but the daemon will report `idle` until the
next attach). Real-world LED verification on multi-app workflows is
tracked separately.

## Build

```sh
nix build         # produces ./result/bin/lazycam
```

## Run

Live mode (talks to OBS at `ws://127.0.0.1:4455`):

```sh
./result/bin/lazycam --device /dev/video10
```

Dry-run mode (logs intended scene switches without contacting OBS):

```sh
./result/bin/lazycam --device /dev/video10 --dry-run
```

Subscribe to the state stream:

```sh
socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/lazycam.sock
```

Trigger an open + close to watch the state machine work:

```sh
cat /dev/video10 > /dev/null & sleep 1 && kill %1
```

You should see one `activate` and one `deactivate` line in the daemon's
log, plus matching `{"state":"active",...}` and `{"state":"idle",...}`
lines on the socket.

## License

MIT.
