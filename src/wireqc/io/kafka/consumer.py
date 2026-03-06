from confluent_kafka import Consumer
from typing import Optional, Dict, Any

from .serde import loads


class KafkaJsonConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
        }
        if extra_config:
            conf.update(extra_config)

        self._c = Consumer(conf)

    def subscribe(self, topics):
        self._c.subscribe(list(topics))

    def poll(self, timeout: float = 1.0):
        msg = self._c.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(msg.error())

        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": loads(msg.value()),
            "timestamp": msg.timestamp(),  # (type, ts_ms)
            "_msg": msg,
        }

    def commit(self, msg=None, asynchronous: bool = True):
        if msg is None:
            self._c.commit(asynchronous=asynchronous)
        else:
            self._c.commit(message=msg["_msg"], asynchronous=asynchronous)

    def close(self):
        self._c.close()