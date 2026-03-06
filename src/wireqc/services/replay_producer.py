import time
import argparse
from pathlib import Path

from wireqc.common.config import load_config
from wireqc.io.offline_loader import iter_process_json
from wireqc.io.kafka.producer import KafkaJsonProducer


def main():
    ap = argparse.ArgumentParser(description="Replay Producer (streaming JSONL, optional realtime pacing)")
    ap.add_argument(
        "--path",
        default=None,
        help="Optional input path. If omitted, prefers data/process_data.json if present, else .json.gz",
    )
    ap.add_argument("--progress-every", type=int, default=5000, help="Print progress every N messages")
    ap.add_argument("--realtime", action="store_true", help="Send at ~10 Hz (sleep 0.1s)")
    ap.add_argument("--sleep", type=float, default=None, help="Custom sleep per message, overrides --realtime")
    ap.add_argument("--max-messages", type=int, default=None, help="Stop after N messages")
    args = ap.parse_args()

    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]
    topic = cfg["topics"]["raw"]

    if args.path is not None:
        path = args.path
    else:
        path = "data/process_data.json" if Path("data/process_data.json").exists() else "data/process_data.json.gz"

    if not Path(path).exists():
        raise FileNotFoundError("Replay input not found: {0}".format(path))

    print("[replay] using input: {0}".format(path))

    producer = KafkaJsonProducer(bs)

    sent = 0
    t0 = time.time()
    last = t0

    for msg in iter_process_json(path):
        key = str(msg.get("MachineId", ""))

        producer.produce(topic=topic, value=msg, key=key)
        sent += 1

        if sent % 1000 == 0:
            try:
                producer._p.poll(0.0)
            except Exception:
                pass

        if args.progress_every and sent % args.progress_every == 0:
            producer.flush(2.0)
            now = time.time()
            rate = args.progress_every / max(now - last, 1e-9)
            print("[replay] sent={0}  rate={1:.1f} msg/s  uptime={2:.1f}s".format(
                sent, rate, now - t0
            ))
            last = now

        sleep_s = args.sleep
        if sleep_s is None and args.realtime:
            sleep_s = 0.1

        if sleep_s:
            time.sleep(sleep_s)

        if args.max_messages is not None and sent >= args.max_messages:
            break

    producer.flush(10.0)
    dt = time.time() - t0
    print("[replay] DONE. sent={0}  avg_rate={1:.1f} msg/s".format(sent, sent / max(dt, 1e-9)))


if __name__ == "__main__":
    main()