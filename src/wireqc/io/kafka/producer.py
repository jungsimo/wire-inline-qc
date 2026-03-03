from confluent_kafka import Producer
from wireqc.io.kafka.serde import dumps


class KafkaJsonProducer:
    def __init__(self, bootstrap_servers: str):
        self._p = Producer({"bootstrap.servers": bootstrap_servers})

    def produce(self, topic: str, value: dict, key: str = None):
        self._p.produce(
            topic=topic,
            key=key.encode("utf-8") if key is not None else None,
            value=dumps(value),
        )

    def flush(self, timeout: float = 5.0):
        self._p.flush(timeout)