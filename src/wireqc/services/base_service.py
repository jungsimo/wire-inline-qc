from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, List, Tuple, Optional
import signal
import time

from wireqc.io.kafka.consumer import KafkaJsonConsumer
from wireqc.io.kafka.producer import KafkaJsonProducer


@dataclass
class ServiceContext:
    cfg: dict
    consumer: KafkaJsonConsumer
    producer: KafkaJsonProducer


class BaseStreamService:
    def __init__(
        self,
        cfg: dict,
        group_id: str,
        in_topics: List[str],
        parse_fn: Callable[[dict], dict],
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = False,
        commit_every: int = 50,
    ):
        self.cfg = cfg
        bs = cfg["kafka"]["bootstrap_servers"]

        self.consumer = KafkaJsonConsumer(
            bootstrap_servers=bs,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
        )
        self.consumer.subscribe(in_topics)

        self.producer = KafkaJsonProducer(bootstrap_servers=bs)
        self.parse_fn = parse_fn
        self._running = True
        self.commit_every = max(commit_every, 1)

        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_):
        self._running = False

    def handle(self, rec: dict) -> Iterable[Tuple[str, dict, Optional[str]]]:
        return []

    def run(self):
        ctx = ServiceContext(self.cfg, self.consumer, self.producer)
        pending_commits = 0
        last_commit_ts = time.monotonic()

        try:
            while self._running:
                msg = self.consumer.poll(0.2)
                if msg is None:
                    if pending_commits > 0 and (time.monotonic() - last_commit_ts) >= 2.0:
                        self.consumer.commit(asynchronous=True)
                        pending_commits = 0
                        last_commit_ts = time.monotonic()
                    continue

                raw = msg["value"]
                rec = self.parse_fn(raw)

                outs = list(self.handle(rec))
                for topic, event, key in outs:
                    self.producer.produce(topic=topic, value=event, key=key)

                if outs:
                    self.producer.flush(timeout=5.0)

                pending_commits += 1
                if pending_commits >= self.commit_every or (time.monotonic() - last_commit_ts) >= 2.0:
                    self.consumer.commit(asynchronous=True)
                    pending_commits = 0
                    last_commit_ts = time.monotonic()

        finally:
            try:
                self.producer.flush(timeout=10.0)
            except Exception:
                pass

            try:
                if pending_commits > 0:
                    self.consumer.commit(asynchronous=False)
            except Exception:
                pass

            try:
                self.consumer.close()
            except Exception:
                pass