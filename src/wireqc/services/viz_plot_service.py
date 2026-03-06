from __future__ import annotations

import argparse
import time
import uuid
from collections import deque

import matplotlib.pyplot as plt

from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer
from wireqc.io.processdata_parser import parse_raw_message


def _make_ephemeral_group(base_group: str, suffix: str) -> str:
    return "{0}-{1}-{2}-{3}".format(base_group, suffix, int(time.time()), uuid.uuid4().hex[:8])


def main():
    ap = argparse.ArgumentParser(description="Inline QC – Live-Visualisierung direkt aus Kafka")
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-viz-dashboard")
    ap.add_argument("--points", type=int, default=800)
    ap.add_argument("--update-hz", type=float, default=5.0)
    ap.add_argument("--downsample", type=int, default=1)
    ap.add_argument("--drain-raw", type=int, default=100)
    ap.add_argument("--drain-evt", type=int, default=50)
    ap.add_argument("--x-window-mm", type=float, default=5000.0)
    ap.add_argument("--xlim-update-every", type=int, default=3)
    args = ap.parse_args()

    cfg = load_config()

    bs = cfg["kafka"]["bootstrap_servers"]
    t_raw = cfg["topics"]["raw"]
    t_profiles = cfg["topics"]["profiles"]
    t_profiles_nio = cfg["topics"]["profiles_nio"]

    c_raw = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=_make_ephemeral_group(args.group, "raw"),
        auto_offset_reset=args.offset,
        enable_auto_commit=False,
    )
    c_raw.subscribe([t_raw])

    c_evt = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=_make_ephemeral_group(args.group, "evt"),
        auto_offset_reset=args.offset,
        enable_auto_commit=False,
    )
    c_evt.subscribe([t_profiles, t_profiles_nio])

    max_points = max(args.points, 100)

    x_len = deque(maxlen=max_points)
    y_dia = deque(maxlen=max_points)
    y_hard = deque(maxlen=max_points)
    y_temp = deque(maxlen=max_points)

    profile_end_positions = deque()
    nio_end_positions = deque()

    total_profiles = 0
    total_nio = 0
    raw_seen = 0

    update_interval = 1.0 / max(args.update_hz, 0.1)
    last_plot_ts = 0.0
    last_stat_ts = 0.0
    draw_idx = 0

    plt.ion()
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(13, 8))
    fig.suptitle("Inline Prozesskontrolle – Live-Visualisierung")
    fig.tight_layout(rect=[0.02, 0.04, 0.98, 0.95])

    try:
        fig.canvas.manager.set_window_title("Inline QC Dashboard")
    except Exception:
        pass

    (line_dia,) = ax1.plot([], [], lw=1.0)
    ax1.set_ylabel("Durchmesser [mm]")
    ax1.set_ylim(11.0, 18.0)
    ax1.grid(True, alpha=0.3)

    (line_hard,) = ax2.plot([], [], lw=1.0)
    ax2.set_ylabel("Härtetemp.")
    ax2.set_ylim(950, 1025)
    ax2.grid(True, alpha=0.3)

    (line_temp,) = ax3.plot([], [], lw=1.0)
    ax3.set_ylabel("Anlasstemp.")
    ax3.set_xlabel("Drahtlänge [mm]")
    ax3.set_ylim(440, 470)
    ax3.grid(True, alpha=0.3)

    counter_text = fig.text(0.99, 0.98, "", ha="right", va="top")

    plt.show(block=False)

    def trim_event_positions(x_min_visible):
        while profile_end_positions and profile_end_positions[0] < x_min_visible:
            profile_end_positions.popleft()
        while nio_end_positions and nio_end_positions[0] < x_min_visible:
            nio_end_positions.popleft()

    try:
        while True:
            evt_count = 0
            while evt_count < args.drain_evt:
                msg = c_evt.poll(0.0)
                if msg is None:
                    break

                value = msg["value"] or {}
                topic = msg["topic"]

                if topic == t_profiles:
                    total_profiles += 1
                    wl = value.get("wire_length_end_mm")
                    if wl is not None:
                        try:
                            profile_end_positions.append(float(wl))
                        except (TypeError, ValueError):
                            pass

                elif topic == t_profiles_nio:
                    total_nio += 1
                    wl = value.get("wire_length_end_mm")
                    if wl is not None:
                        try:
                            nio_end_positions.append(float(wl))
                        except (TypeError, ValueError):
                            pass

                evt_count += 1

            raw_count = 0
            while raw_count < args.drain_raw:
                msg = c_raw.poll(0.0)
                if msg is None:
                    break

                rec = parse_raw_message(msg["value"])
                wire_length = rec.get("wire_length_mm")
                if wire_length is None:
                    raw_count += 1
                    continue

                raw_seen += 1

                if raw_seen % max(args.downsample, 1) != 0:
                    raw_count += 1
                    continue

                try:
                    x_val = float(wire_length)
                except (TypeError, ValueError):
                    raw_count += 1
                    continue

                x_len.append(x_val)

                diameter = rec.get("diameter")
                hard_temp = rec.get("hard_temp")
                temp_temp = rec.get("temp_temp")

                try:
                    y_dia.append(float(diameter) if diameter is not None else float("nan"))
                except (TypeError, ValueError):
                    y_dia.append(float("nan"))

                try:
                    y_hard.append(float(hard_temp) if hard_temp is not None else float("nan"))
                except (TypeError, ValueError):
                    y_hard.append(float("nan"))

                try:
                    y_temp.append(float(temp_temp) if temp_temp is not None else float("nan"))
                except (TypeError, ValueError):
                    y_temp.append(float("nan"))

                if args.x_window_mm is not None and len(x_len) >= 2:
                    keep_from = x_len[-1] - float(args.x_window_mm)
                    while x_len and x_len[0] < keep_from:
                        x_len.popleft()
                        y_dia.popleft()
                        y_hard.popleft()
                        y_temp.popleft()

                raw_count += 1

            now = time.time()

            if now - last_stat_ts >= 2.0:
                print("[viz] raw_seen={0} profiles_total={1} profiles_nio_total={2} buffer={3}".format(
                    raw_seen, total_profiles, total_nio, len(x_len)
                ))
                last_stat_ts = now

            if len(x_len) >= 2 and (now - last_plot_ts) >= update_interval:
                draw_idx += 1

                x_right = x_len[-1]
                x_left = max(x_len[0], x_right - float(args.x_window_mm))
                span = max(x_right - x_left, 1.0)
                pad = max(span * 0.02, 1.0)

                x_min_visible = x_left - pad
                x_max_visible = x_right + pad

                trim_event_positions(x_min_visible)

                line_dia.set_data(list(x_len), list(y_dia))
                line_hard.set_data(list(x_len), list(y_hard))
                line_temp.set_data(list(x_len), list(y_temp))

                if draw_idx == 1 or (draw_idx % max(args.xlim_update_every, 1) == 0):
                    ax3.set_xlim(x_min_visible, x_max_visible)

                counter_text.set_text(
                    "COUNTERS\n"
                    "Rohdaten gesehen:      {0}\n\n"
                    "Profile gesamt:        {1}\n"
                    "Profile n.i.O. gesamt: {2}\n\n"
                    "Profile im Fenster:    {3}\n"
                    "n.i.O. im Fenster:     {4}\n\n"
                    "Downsample:            1/{5}\n"
                    "GUI-Update:            {6:.1f} Hz".format(
                        raw_seen,
                        total_profiles,
                        total_nio,
                        len(profile_end_positions),
                        len(nio_end_positions),
                        max(args.downsample, 1),
                        args.update_hz,
                    )
                )

                fig.canvas.draw()
                plt.pause(0.001)

                last_plot_ts = now
            else:
                plt.pause(0.001)

    except KeyboardInterrupt:
        print("\n[viz] Beendet durch Benutzer.")
    finally:
        try:
            c_raw.close()
        except Exception:
            pass
        try:
            c_evt.close()
        except Exception:
            pass
        try:
            plt.close(fig)
        except Exception:
            pass


if __name__ == "__main__":
    main()