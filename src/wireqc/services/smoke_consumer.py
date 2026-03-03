from wireqc.common.config import load_config
from wireqc.io.kafka.consumer import KafkaJsonConsumer
from wireqc.io.processdata_parser import parse_raw_message

def main():
    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]
    topic = cfg["topics"]["raw"]

    c = KafkaJsonConsumer(
        bootstrap_servers=bs,
        group_id="wireqc-smoke",
        auto_offset_reset=cfg["consumer"]["auto_offset_reset"],
        enable_auto_commit=cfg["consumer"]["enable_auto_commit"],
    )
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            raw = msg["value"]
            rec = parse_raw_message(raw)
            print(rec)
            c.commit(msg)
    finally:
        c.close()

if __name__ == "__main__":
    main()