from __future__ import annotations

import argparse
import time
from collections import deque

import matplotlib.pyplot as plt

from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer
from wireqc.io.processdata_parser import parse_raw_message


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-viz-plot")
    ap.add_argument("--points", type=int, default=8000, help="ringbuffer size")
    args = ap.parse_args()

    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]
    t_raw = cfg["topics"]["raw"]
    t_profiles = cfg["topics"]["profiles"]
    t_nio = cfg["topics"]["profiles_nio"]

    c = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=args.group,
        auto_offset_reset=args.offset,
        enable_auto_commit=cfg["consumer"]["enable_auto_commit"],
    )
    c.subscribe([t_raw, t_profiles, t_nio])

    N = args.points
    x_len = deque(maxlen=N)
    y_dia = deque(maxlen=N)
    y_hard = deque(maxlen=N)
    y_temp = deque(maxlen=N)

    total_profiles = 0
    total_nio = 0
    raw_seen = 0

    plt.ion()

    # Figure 1: Diameter
    fig1, ax1 = plt.subplots()
    line1, = ax1.plot([], [])
    ax1.set_xlabel("Drahtlänge [mm]")
    ax1.set_ylabel("Durchmesser [mm]")
    ax1.set_title("Inline QC – Durchmesser über Drahtlänge")

    # Figure 2: Hardening temperature
    fig2, ax2 = plt.subplots()
    line2, = ax2.plot([], [])
    ax2.set_xlabel("Drahtlänge [mm]")
    ax2.set_ylabel("Härtetemperatur")
    ax2.set_title("Inline QC – Härtetemperatur über Drahtlänge")

    # Figure 3: Tempering temperature
    fig3, ax3 = plt.subplots()
    line3, = ax3.plot([], [])
    ax3.set_xlabel("Drahtlänge [mm]")
    ax3.set_ylabel("Anlasstemperatur")
    ax3.set_title("Inline QC – Anlasstemperatur über Drahtlänge")

    for f in (fig1, fig2, fig3):
        f.show()

    last_plot = 0.0
    last_stat = 0.0

    try:
        while True:
            msg = c.poll(0.2)
            if msg is None:
                plt.pause(0.001)
                continue

            if msg["topic"] == t_raw:
                rec = parse_raw_message(msg["value"])
                L = rec.get("wire_length_mm")
                D = rec.get("diameter")
                H = rec.get("hard_temp")
                T = rec.get("temp_temp")

                if L is not None:
                    x_len.append(float(L))
                    y_dia.append(float(D) if D is not None else float("nan"))
                    y_hard.append(float(H) if H is not None else float("nan"))
                    y_temp.append(float(T) if T is not None else float("nan"))
                    raw_seen += 1

            elif msg["topic"] == t_profiles:
                total_profiles += 1
            elif msg["topic"] == t_nio:
                total_nio += 1

            c.commit(msg)

            now = time.time()

            # status every 2s
            if now - last_stat >= 2.0:
                print(f"[viz_plot] raw={raw_seen} profiles={total_profiles} nio={total_nio}")
                last_stat = now

            # update plots ~2 Hz
            if now - last_plot >= 0.5 and len(x_len) >= 2:
                # Diameter
                line1.set_data(x_len, y_dia)
                ax1.relim(); ax1.autoscale_view()
                ax1.set_title(f"Durchmesser | raw={raw_seen} profiles={total_profiles} nio={total_nio}")

                # Hard temp
                line2.set_data(x_len, y_hard)
                ax2.relim(); ax2.autoscale_view()
                ax2.set_title(f"Härtetemperatur | raw={raw_seen} profiles={total_profiles} nio={total_nio}")

                # Tempering temp
                line3.set_data(x_len, y_temp)
                ax3.relim(); ax3.autoscale_view()
                ax3.set_title(f"Anlasstemperatur | raw={raw_seen} profiles={total_profiles} nio={total_nio}")

                fig1.canvas.draw_idle()
                fig2.canvas.draw_idle()
                fig3.canvas.draw_idle()
                plt.pause(0.001)

                last_plot = now

    finally:
        c.close()


if __name__ == "__main__":
    main()