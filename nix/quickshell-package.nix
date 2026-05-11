# Packages the LazycamIndicator QML directory into a nix store output
# under `share/lazycam-indicator/`, suitable for Quickshell-based shells
# (DankMaterialShell, etc.) to import via a stable nix-store path.
#
# Usage (in DMS or similar):
#     import "/nix/store/<hash>-lazycam-indicator-1.0.0/share/lazycam-indicator" as Lazycam
#     ...
#     Lazycam.LazycamIndicator { }
#
# Or via Quickshell's QmlImport / ScopedQML, depending on the shell's
# import surface — Quickshell's "modules" are just import paths.
{
  stdenv,
  lib,
}:
stdenv.mkDerivation {
  pname = "lazycam-indicator";
  version = "1.0.0";

  src = ../quickshell/lazycam-indicator;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    runHook preInstall
    mkdir -p $out/share/lazycam-indicator
    install -m 0644 -t $out/share/lazycam-indicator \
      $src/LazycamIndicator.qml \
      $src/qmldir
    runHook postInstall
  '';

  meta = {
    description = "Quickshell QML indicator widget for the lazycam daemon";
    homepage = "https://github.com/joshsymonds/lazycam";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
