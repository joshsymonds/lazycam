# lazycam home-manager module.
#
# Wires the lazycam daemon as a systemd --user service tied to
# graphical-session.target — meaning it starts when the user logs into
# their graphical session and shuts down cleanly on logout.
#
# Consumer pattern (in nix-config):
#
#   {
#     imports = [ inputs.lazycam.homeManagerModules.default ];
#     services.lazycam = {
#       enable = true;
#       package = inputs.lazycam.packages.${pkgs.system}.default;
#       device = "/dev/video10";
#       obsUrl = "ws://127.0.0.1:4455";
#     };
#   }
#
# Either pass `package = ...` explicitly OR add lazycam to your nixpkgs
# overlay so `pkgs.lazycam` resolves. The module's default reaches for
# `pkgs.lazycam` and throws a helpful error if not present, since a
# home-manager module can't reach flake inputs without specialArgs.
{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.lazycam;

  # Compose the ExecStart command from the typed options. lib.escapeShellArg
  # quotes each value so paths with spaces / shell metacharacters survive
  # the systemd-unit -> shell parse.
  execArgs = lib.concatStringsSep " " (lib.filter (s: s != "") [
    "--device=${lib.escapeShellArg cfg.device}"
    "--obs-url=${lib.escapeShellArg cfg.obsUrl}"
    "--scene-active=${lib.escapeShellArg cfg.sceneActive}"
    "--scene-standby=${lib.escapeShellArg cfg.sceneStandby}"
    (lib.optionalString (cfg.stateSocket != null)
      "--state-socket=${lib.escapeShellArg cfg.stateSocket}")
    (lib.optionalString (cfg.cameraSource != "")
      "--camera-source=${lib.escapeShellArg cfg.cameraSource}")
    "--camera-device=${lib.escapeShellArg cfg.cameraDevice}"
    (lib.optionalString (cfg.excludeComms != [])
      "--exclude-comms=${lib.escapeShellArg (lib.concatStringsSep "," cfg.excludeComms)}")
    (lib.optionalString cfg.dryRun "--dry-run")
    (lib.optionalString cfg.debug "--debug")
  ]);
in {
  options.services.lazycam = {
    enable = lib.mkEnableOption "lazycam, on-demand v4l2 producer gating for OBS";

    package = lib.mkOption {
      type = lib.types.package;
      default =
        pkgs.lazycam or (throw ''
          services.lazycam: `pkgs.lazycam` is not in scope.

          Either:
            1. Pass the package explicitly:
                 services.lazycam.package = inputs.lazycam.packages.\${pkgs.system}.default;
            2. Add lazycam to your nixpkgs overlay so `pkgs.lazycam` resolves.

          A home-manager module cannot reach flake inputs without
          specialArgs, so we cannot do this for you here.
        '');
      defaultText = lib.literalExpression "pkgs.lazycam";
      description = "The lazycam package to run.";
    };

    device = lib.mkOption {
      type = lib.types.str;
      default = "/dev/video10";
      example = "/dev/video10";
      description = "v4l2 device path the daemon watches for opens/closes.";
    };

    obsUrl = lib.mkOption {
      type = lib.types.str;
      default = "ws://127.0.0.1:4455";
      example = "ws://127.0.0.1:4455";
      description = ''
        OBS WebSocket v5 endpoint, in either `ws://host:port` or bare
        `host:port` form. Loopback only; lazycam disables OBS WebSocket
        auth by design (UNIX socket on /run/user is the access boundary).
      '';
    };

    sceneActive = lib.mkOption {
      type = lib.types.str;
      default = "Active";
      example = "Active";
      description = ''
        OBS scene name lazycam switches to when the first consumer
        attaches to the v4l2 loopback device. Must exist in the user's
        scene collection.
      '';
    };

    sceneStandby = lib.mkOption {
      type = lib.types.str;
      default = "Standby";
      example = "Standby";
      description = ''
        OBS scene name lazycam switches to when the last consumer
        releases the v4l2 loopback device. Should NOT include any
        video-capture-device source so OBS releases the real /dev/video0
        handle and the hardware LED goes off.
      '';
    };

    stateSocket = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "/run/user/1000/lazycam.sock";
      description = ''
        Optional UNIX socket path where lazycam publishes newline-
        delimited JSON state events for external indicators. When null,
        the daemon defaults to $XDG_RUNTIME_DIR/lazycam.sock at runtime.
      '';
    };

    cameraSource = lib.mkOption {
      type = lib.types.str;
      default = "";
      example = "Real Webcam";
      description = ''
        OBS input source name to device-id-gate via SetInputSettings.
        Empty disables device-level gating — leave empty if the
        Standby/Active scene swap alone is sufficient for your use
        case, set to the source name when the underlying camera plugin
        keeps the device open across scenes (OBS's v4l2_input plugin
        in particular has no show/hide hooks, so without this gate
        the hardware LED stays lit while OBS runs regardless of
        which scene is the program scene).
      '';
    };

    cameraDevice = lib.mkOption {
      type = lib.types.str;
      default = "/dev/video0";
      example = "/dev/video0";
      description = ''
        v4l2 device path written to the gated OBS source on Activate
        transitions. On Deactivate the source's device_id is cleared,
        which makes OBS's v4l2 plugin release the prior file
        descriptor — the kernel turns off the hardware LED.
      '';
    };

    excludeComms = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      example = [".obs-wrapped"];
      description = ''
        Process comm strings whose opens of the watched device are NOT
        counted toward the consumer ref-count.

        Required for hosts that auto-start OBS + the OBS Virtual
        Camera output: OBS itself opens the v4l2 loopback as a
        producer, which would otherwise pin lazycam in the Active
        state for as long as OBS runs (defeating the LED-off
        invariant). Listing OBS's comm here makes lazycam treat that
        open as background noise, so the LED only lights when
        something other than OBS (Zoom, Meet, ffmpeg, ...) attaches
        to the loopback.

        comm strings come from `/proc/PID/comm` (truncated to 15
        chars by TASK_COMM_LEN). On a nix-wrapped OBS install the
        value is typically `.obs-wrapped`; verify on your system with
            cat /proc/$(pgrep -f obs | head -1)/comm

        Default is the empty list — every opener counts. This
        preserves backwards-compatible behavior on hosts that don't
        auto-start OBS.
      '';
    };

    dryRun = lib.mkOption {
      type = lib.types.bool;
      default = false;
      example = false;
      description = ''
        Log intended scene transitions instead of contacting OBS.
        Useful for testing the state pipeline without a live OBS.
      '';
    };

    debug = lib.mkOption {
      type = lib.types.bool;
      default = false;
      example = false;
      description = ''
        Log every inotify event (otherwise only 0↔N transitions are
        logged). Verbose; intended for troubleshooting.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.user.services.lazycam = {
      Unit = {
        Description = "lazycam — on-demand v4l2 producer gating for OBS virtual-cam privacy";
        Documentation = ["https://github.com/joshsymonds/lazycam"];
        PartOf = ["graphical-session.target"];
        After = ["graphical-session.target"];
      };
      Service = {
        ExecStart = "${lib.getExe cfg.package} ${execArgs}";
        Restart = "on-failure";
        RestartSec = 2;
        # The daemon needs read access to the v4l2 device; on most
        # distros /dev/video* is group=video and the user is already in
        # that group. No extra capabilities needed for inotify or the
        # UNIX socket.
      };
      Install = {
        WantedBy = ["graphical-session.target"];
      };
    };
  };
}
