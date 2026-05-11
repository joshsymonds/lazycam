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

Pre-alpha. This iteration logs kernel events but does not yet talk to
OBS or maintain a consumer ref-count. See the design epic for the full
roadmap.

## Build

```sh
nix build         # produces ./result/bin/lazycam
```

## Run

```sh
./result/bin/lazycam --device /dev/video10
```

In another terminal, trigger an open + close:

```sh
cat /dev/video10 > /dev/null & sleep 1 && kill %1
```

You should see one `IN_OPEN` event and one `IN_CLOSE_NOWRITE` event in
the daemon's log.

## License

MIT.
