# src/tools/create_topics.py
from __future__ import annotations

import time
from confluent_kafka.admin import AdminClient, NewTopic
from wireqc.common.config import load_config


def ensure_topics(bootstrap: str, topics: list[str], partitions: int = 1, rf: int = 1, timeout_s: int = 20):
    admin = AdminClient({"bootstrap.servers": bootstrap})

    # check existing
    md = admin.list_topics(timeout=timeout_s)
    existing = set(md.topics.keys())

    to_create = [t for t in topics if t not in existing]
    if not to_create:
        print("[topics] all topics already exist")
        return

    print("[topics] creating:", ", ".join(to_create))
    fs = admin.create_topics([NewTopic(t, num_partitions=partitions, replication_factor=rf) for t in to_create])

    # wait for results
    for t, f in fs.items():
        try:
            f.result(timeout=timeout_s)
            print(f"[topics] created: {t}")
        except Exception as e:
            # TopicAlreadyExists etc. -> ok for idempotency
            print(f"[topics] {t}: {e}")

    # small delay for metadata propagation
    time.sleep(1.0)


def main():
    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]
    topics = [
        cfg["topics"]["raw"],
        cfg["topics"]["alarms"],
        cfg["topics"]["corr"],
        cfg["topics"]["profiles"],
        cfg["topics"]["profiles_nio"],
    ]
    ensure_topics(bs, topics)


if __name__ == "__main__":
    main()