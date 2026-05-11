// LazycamIndicator — a small QML widget that connects to lazycam's
// UNIX socket and renders a colored dot reflecting the current camera
// state: red when active (camera fd held, hardware LED on), gray when
// idle.
//
// Typical use from a Quickshell bar (e.g. DankMaterialShell):
//
//     import "/nix/store/.../share/lazycam-indicator" as Lazycam
//     ...
//     Lazycam.LazycamIndicator {
//         size: 12
//     }
//
// The widget owns its own socket connection. Multiple instances on the
// same shell will each open an independent subscription — fine, the
// lazycam publisher broadcasts to all subscribers.
//
// Snapshot-on-connect (the protocol's load-bearing feature) means a
// freshly-instantiated widget renders the correct state without
// waiting for the next transition. If the daemon is down at startup,
// the widget stays in its `unknownColor` state and the socket retries
// every `reconnectIntervalMs` until the daemon is reachable.

// pragma ComponentBehavior: Bound lets nested components (the
// SplitParser inside the Socket) capture the outer `root` id cleanly
// — required since Qt 6.5 for unqualified access across nested scopes.
pragma ComponentBehavior: Bound

import QtQuick
import Quickshell.Io

Item {
    id: root

    // Knobs the consumer can override. Sensible defaults so the
    // common case is `LazycamIndicator { }`.
    property int size: 12
    property color activeColor: "#ff4444"
    property color idleColor: "#666666"
    property color unknownColor: "#444444"  // daemon unreachable
    property string socketPath: Quickshell.env("XDG_RUNTIME_DIR") + "/lazycam.sock"
    property int reconnectIntervalMs: 1000

    // Public read-only state. QML auto-generates the `stateChanged` and
    // `refCountChanged` signals consumers subscribe to via `onStateChanged`.
    readonly property string state: _state
    readonly property int refCount: _refCount

    // Internal mutable state. Underscore-prefixed by convention so the
    // distinction from the public `state` is visible at the call site.
    property string _state: "unknown"
    property int _refCount: 0

    implicitWidth: size
    implicitHeight: size

    Rectangle {
        anchors.fill: parent
        radius: width / 2
        color: {
            switch (root._state) {
                case "active": return root.activeColor
                case "idle":   return root.idleColor
                default:       return root.unknownColor
            }
        }

        // Subtle highlight when active so the red dot reads as "on"
        // even on dark bar backgrounds.
        Rectangle {
            anchors.centerIn: parent
            width: parent.width * 0.4
            height: parent.height * 0.4
            radius: width / 2
            color: Qt.lighter(parent.color, 1.4)
            visible: root._state === "active"
        }
    }

    Socket {
        id: socket
        path: root.socketPath
        connected: true

        parser: SplitParser {
            splitMarker: "\n"
            onRead: line => root._handleLine(line)
        }

        onConnectionStateChanged: {
            if (!socket.connected) {
                // Daemon is unreachable or just released us. Reset our
                // state so we don't lie to the user; the reconnect
                // timer will try again shortly. Assigning to _state
                // fires stateChanged() automatically via QML's
                // property-change-signal generation.
                root._state = "unknown"
                root._refCount = 0
                reconnectTimer.restart()
            }
        }
    }

    Timer {
        id: reconnectTimer
        interval: root.reconnectIntervalMs
        repeat: false
        onTriggered: socket.connected = true
    }

    function _handleLine(line) {
        if (!line) return
        try {
            const ev = JSON.parse(line)
            if (typeof ev.state === "string") {
                _state = ev.state
            }
            if (typeof ev.refCount === "number") {
                _refCount = ev.refCount
            }
            // Property assignments above auto-fire stateChanged /
            // refCountChanged. No explicit emit needed.
        } catch (e) {
            // Malformed line — daemon protocol skew or a partial frame.
            // Ignore silently; the next valid line will resync state.
        }
    }
}
