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

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            print(f"{msg['topic']}@{msg['partition']}:{msg['offset']} key={msg['key']} value={msg['value']}")
            c.commit(msg)
    finally:
        c.close()

if __name__ == "__main__":
    main()