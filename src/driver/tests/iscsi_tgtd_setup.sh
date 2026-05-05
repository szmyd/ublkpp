#!/usr/bin/env bash
# Manage a transient tgtd instance for iSCSIDisk tests.
#
# Usage: iscsi_tgtd_setup.sh start|stop
#
# Starts tgtd in the foreground (backgrounded by us) bound to 127.0.0.1:13260
# for the iSCSI portal and 13261 for the management channel (--control-port,
# avoids tgtd's hardcoded /var/run/tgtd unix socket). Single sparse-file-backed
# LUN exported as iqn.2026-05.test.ublkpp:lun0.
#
# Soft-skip exit codes (exit 0): tgtd missing, port squatted by non-tgtd
# process, lock-file inaccessible without sudo. Combined with the C++ test's
# runtime port probe + GTEST_SKIP, a soft-skip surfaces as Skipped (green).

set -eu

PORT="${ISCSI_TEST_PORT:-13260}"
CTRL_PORT="${ISCSI_TEST_CTRL_PORT:-13261}"
STATE_DIR="${ISCSI_TEST_STATE:-/tmp/ublkpp_iscsi_test}"
PID_FILE="$STATE_DIR/tgtd.pid"
BACKING_SIZE="${ISCSI_TEST_LUN_SIZE:-256M}"
# Number of LUNs to provision. RAID10 composition tests need 4; the single-LUN
# FunctionalISCSI test just uses LUN 1.
LUN_COUNT="${ISCSI_TEST_LUN_COUNT:-4}"
TARGET_IQN="iqn.2026-05.test.ublkpp:lun0"

start() {
    if ! command -v tgtd >/dev/null || ! command -v tgtadm >/dev/null; then
        echo "ISCSI_SOFT_SKIP: tgtd/tgtadm not in PATH"
        exit 0
    fi

    # tgtd writes its per-port lock file at /var/run/tgtd/socket.<CTRL_PORT>.lock
    # (path is hardcoded; --control-port only changes the suffix). The directory
    # must exist and be writable. /var/run is tmpfs, so a chown disappears on
    # reboot — for a persistent dev-box setup, prefer a tmpfiles.d entry. Three
    # ways to satisfy this:
    #   1. Already root (CI container path).
    #   2. The directory exists and is writable by us (dev-box path).
    #   3. Passwordless sudo is available (CI runner path) — we mkdir on demand.
    # If none apply, soft-skip with an actionable hint.
    SUDO=""
    IPC_DIR="/var/run/tgtd"
    if [[ $EUID -eq 0 ]]; then
        mkdir -p "$IPC_DIR"
    elif [[ -d "$IPC_DIR" && -w "$IPC_DIR" ]]; then
        : # user can write into the dir; run tgtd as user
    elif command -v sudo >/dev/null && sudo -n true 2>/dev/null; then
        SUDO="sudo"
        $SUDO mkdir -p "$IPC_DIR"
    else
        cat >&2 <<EOF
ISCSI_SOFT_SKIP: tgtd needs $IPC_DIR to be writable (or root / passwordless sudo).
Per-boot one-shot:
    sudo mkdir -p $IPC_DIR && sudo chown \$USER $IPC_DIR
Persistent (survives reboot — /var/run is tmpfs):
    echo 'd $IPC_DIR 0755 \$USER \$USER -' | sudo tee /etc/tmpfiles.d/tgtd.conf
    sudo systemd-tmpfiles --create /etc/tmpfiles.d/tgtd.conf
Soft-skipping iSCSIDisk tests for this run.
EOF
        exit 0
    fi

    if [[ -f "$PID_FILE" ]] && $SUDO kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "tgtd already running (pid $(cat "$PID_FILE"))"
        return 0
    fi

    # Pre-flight: an earlier failed run may have left a tgtd squatting on our
    # port. tgtd is owned by root when launched via sudo, so ss without sudo
    # may hide the pid; route ss through $SUDO to make sure we can identify it.
    if command -v ss >/dev/null; then
        squatter=$($SUDO ss -Hltnp "sport = :$PORT" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
        if [[ -n "$squatter" ]]; then
            squatter_name=$(cat "/proc/$squatter/comm" 2>/dev/null || echo unknown)
            if [[ "$squatter_name" == "tgtd" ]]; then
                echo "Killing stale tgtd (pid $squatter) bound to port $PORT"
                $SUDO kill "$squatter" 2>/dev/null || true
                for _ in $(seq 1 50); do
                    $SUDO kill -0 "$squatter" 2>/dev/null || break
                    sleep 0.1
                done
                $SUDO kill -9 "$squatter" 2>/dev/null || true
            else
                echo "ISCSI_SOFT_SKIP: Port $PORT is in use by '$squatter_name' (pid $squatter)"
                exit 0
            fi
        fi
    fi

    mkdir -p "$STATE_DIR"
    for i in $(seq 1 "$LUN_COUNT"); do
        truncate -s "$BACKING_SIZE" "$STATE_DIR/lun${i}.img"
    done

    $SUDO tgtd \
        --foreground \
        --iscsi portal=127.0.0.1:"$PORT" \
        --control-port "$CTRL_PORT" \
        >"$STATE_DIR/tgtd.log" 2>&1 &
    echo $! >"$PID_FILE"

    # Wait until tgtadm can reach the control port. Probing tgtadm directly is
    # more reliable than watching for a unix-socket path.
    for _ in $(seq 1 300); do
        if $SUDO tgtadm --control-port "$CTRL_PORT" --lld iscsi --op show --mode target >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    if ! $SUDO tgtadm --control-port "$CTRL_PORT" --lld iscsi --op show --mode target >/dev/null 2>&1; then
        echo "ISCSI_SOFT_SKIP: tgtd failed to come up; log follows:"
        cat "$STATE_DIR/tgtd.log" || true
        $SUDO kill "$(cat "$PID_FILE")" 2>/dev/null || true
        # Wait for tgtd to actually die — otherwise it keeps the port and
        # the test gets a confusing "Connection reset by peer" instead of
        # a clean GTEST_SKIP via failed connect().
        for _ in $(seq 1 50); do
            $SUDO kill -0 "$(cat "$PID_FILE")" 2>/dev/null || break
            sleep 0.1
        done
        $SUDO kill -9 "$(cat "$PID_FILE")" 2>/dev/null || true
        exit 0  # soft-skip; the C++ probe will GTEST_SKIP all tests
    fi

    $SUDO tgtadm --control-port "$CTRL_PORT" --lld iscsi --op new --mode target --tid 1 -T "$TARGET_IQN"
    for i in $(seq 1 "$LUN_COUNT"); do
        $SUDO tgtadm --control-port "$CTRL_PORT" --lld iscsi --op new --mode logicalunit --tid 1 \
            --lun "$i" -b "$STATE_DIR/lun${i}.img"
    done
    $SUDO tgtadm --control-port "$CTRL_PORT" --lld iscsi --op bind --mode target --tid 1 -I ALL
}

stop() {
    SUDO=""
    if [[ $EUID -ne 0 ]] && command -v sudo >/dev/null && sudo -n true 2>/dev/null; then
        SUDO="sudo"
    fi

    # Find tgtd by its listening socket, not PID_FILE: when start() ran tgtd via
    # sudo, $! was sudo's PID (sudo forks for PAM/audit proxying on most distros)
    # and SIGTERM to sudo doesn't always reach the child. Killing the actual
    # listener is reliable.
    pid=""
    if command -v ss >/dev/null; then
        pid=$($SUDO ss -Hltnp "sport = :$PORT" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
    fi
    if [[ -n "$pid" ]]; then
        $SUDO kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            $SUDO kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        $SUDO kill -9 "$pid" 2>/dev/null || true
    fi

    # Best-effort kill of the recorded PID too, in case tgtd already exited
    # but a sudo wrapper is still around.
    if [[ -f "$PID_FILE" ]]; then
        $SUDO kill "$(cat "$PID_FILE")" 2>/dev/null || true
    fi

    rm -f "$PID_FILE" "$STATE_DIR"/lun*.img
    # Intentionally leave $STATE_DIR/tgtd.log in place for post-mortem; the
    # next start() truncates the log via the daemon redirect, so this won't
    # accumulate across runs.
}

case "${1:-}" in
    start) start ;;
    stop)  stop ;;
    *) echo "Usage: $0 start|stop" >&2; exit 1 ;;
esac
