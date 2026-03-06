from __future__ import annotations

import argparse
import time

from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-viz")
    args = ap.parse_args()

    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]

    t_profiles = cfg["topics"]["profiles"]
    t_profiles_nio = cfg["topics"]["profiles_nio"]

    c = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=args.group,
        auto_offset_reset=args.offset,
        enable_auto_commit=False,
    )
    c.subscribe([t_profiles, t_profiles_nio])

    total_profiles = 0
    total_nio = 0
    last_print = 0.0
    last_wait_print = 0.0
    waiting = False

    try:
        while True:
            try:
                msg = c.poll(1.0)
            except RuntimeError as e:
                s = str(e)
                if "UNKNOWN_TOPIC_OR_PART" in s or "Unknown topic or partition" in s:
                    now = time.time()
                    if (not waiting) or (now - last_wait_print) >= 5.0:
                        print("[viz] Warte auf Topics (profiles / profiles_nio)...")
                        waiting = True
                        last_wait_print = now
                    time.sleep(1.0)
                    continue
                raise

            if msg is None:
                continue

            if waiting:
                print("[viz] Topics verfügbar – starte Zählung.")
                waiting = False

            if msg["topic"] == t_profiles:
                total_profiles += 1
            elif msg["topic"] == t_profiles_nio:
                total_nio += 1

            now = time.time()
            if now - last_print >= 1.0:
                print("[viz] profiles={0}  nio={1}".format(total_profiles, total_nio))
                last_print = now

    finally:
        c.close()


if __name__ == "__main__":
    main()