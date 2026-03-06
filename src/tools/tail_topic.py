import argparse
import time

from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True, help="Topic name to tail")
    ap.add_argument("--group", default="wireqc-tail", help="Consumer group id")
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    args = ap.parse_args()

    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]

    c = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=args.group,
        auto_offset_reset=args.offset,
        enable_auto_commit=False,
    )
    c.subscribe([args.topic])

    waiting_for_topic = False
    last_wait_print = 0.0

    try:
        while True:
            try:
                msg = c.poll(1.0)
            except RuntimeError as e:
                s = str(e)
                if "UNKNOWN_TOPIC_OR_PART" in s or "Unknown topic or partition" in s:
                    now = time.time()
                    if not waiting_for_topic or (now - last_wait_print) >= 5.0:
                        print("[tail] Topic '{0}' noch nicht verfügbar – warte...".format(args.topic))
                        waiting_for_topic = True
                        last_wait_print = now
                    time.sleep(1.0)
                    continue
                raise

            if msg is None:
                continue

            if waiting_for_topic:
                print("[tail] Topic '{0}' ist verfügbar – starte Ausgabe.".format(args.topic))
                waiting_for_topic = False

            print("{0}@{1}:{2} key={3} value={4}".format(
                msg["topic"], msg["partition"], msg["offset"], msg["key"], msg["value"]
            ))
    finally:
        c.close()


if __name__ == "__main__":
    main()