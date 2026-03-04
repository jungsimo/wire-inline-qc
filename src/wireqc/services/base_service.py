# src/wireqc/services/base_service.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Iterable
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
        in_topics: list[str],
        parse_fn: Callable[[dict], dict],
    ):
        self.cfg = cfg
        bs = cfg["kafka"]["bootstrap_servers"]

        self.consumer = KafkaJsonConsumer(
            bootstrap_servers=bs,
            group_id=group_id,
            auto_offset_reset=cfg["consumer"].get("auto_offset_reset", "earliest"),
            enable_auto_commit=cfg["consumer"].get("enable_auto_commit", False),
        )
        self.consumer.subscribe(in_topics)
        self.producer = KafkaJsonProducer(bootstrap_servers=bs)
        self.parse_fn = parse_fn
        self._running = True

        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_):
        self._running = False

    def handle(self, rec: dict) -> Iterable[tuple[str, dict, str | None]]:
        """
        Return iterable of (topic, event_dict, key_str_or_None)
        """
        return []

    def run(self):
        ctx = ServiceContext(self.cfg, self.consumer, self.producer)

        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                raw = msg["value"]
                rec = self.parse_fn(raw)

                # business logic → outputs
                outs = list(self.handle(rec))
                for topic, event, key in outs:
                    self.producer.produce(topic=topic, value=event, key=key)

                # ensure delivery before commit (simple & safe for 10 Hz)
                if outs:
                    self.producer.flush(timeout=5.0)

                self.consumer.commit(msg)

        finally:
            try:
                self.consumer.close()
            except Exception:
                pass