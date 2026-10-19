"""Thin confluent-kafka helpers shared by the producers."""

from __future__ import annotations

import uuid
from typing import Any

import orjson
from confluent_kafka import Consumer, Producer

from vimana_common.settings import get_settings


def make_producer(**overrides: Any) -> Producer:
    conf = {
        "bootstrap.servers": get_settings().kafka_bootstrap,
        "linger.ms": 50,
        "compression.type": "lz4",
        "enable.idempotence": True,
    }
    conf.update(overrides)
    return Producer(conf)


def make_control_consumer(topic: str = "sim.control") -> Consumer:
    """Consumer that only sees commands issued after it starts."""
    consumer = Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap,
            "group.id": f"vimana-sim-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def drain_commands(consumer: Consumer, max_messages: int = 50) -> list[dict]:
    """Non-blocking read of pending control commands."""
    out: list[dict] = []
    for msg in consumer.consume(num_messages=max_messages, timeout=0.01):
        if msg.error() or msg.value() is None:
            continue
        try:
            out.append(orjson.loads(msg.value()))
        except orjson.JSONDecodeError:
            continue
    return out


def send_json(producer: Producer, topic: str, key: str | None, payload: dict) -> None:
    producer.produce(topic, key=key.encode() if key else None, value=orjson.dumps(payload))
    producer.poll(0)
