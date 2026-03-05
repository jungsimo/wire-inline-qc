# src/tools/tail_topic.py
import argparse
from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True, help="Topic name to tail")
    ap.add_argument("--group", default="wireqc-tail", help="Consumer group id")
    args = ap.parse_args()

    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]

    c = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id=args.group,
        auto_offset_reset=cfg["consumer"]["auto_offset_reset"],
        enable_auto_commit=cfg["consumer"]["enable_auto_commit"],
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
                    # Status nur selten ausgeben (alle 5s), damit das Terminal nicht spammt
                    if not waiting_for_topic or (now - last_wait_print) >= 5.0:
                        print(f"[tail] Topic '{args.topic}' noch nicht verfügbar – warte...")
                        waiting_for_topic = True
                        last_wait_print = now
                    time.sleep(1.0)
                    continue
                raise

            if msg is None:
                continue

            if waiting_for_topic:
                print(f"[tail] Topic '{args.topic}' ist verfügbar – starte Ausgabe.")
                waiting_for_topic = False

            print(f"{msg['topic']}@{msg['partition']}:{msg['offset']} key={msg['key']} value={msg['value']}")
            c.commit(msg)
    finally:
        c.close()

if __name__ == "__main__":
    main()