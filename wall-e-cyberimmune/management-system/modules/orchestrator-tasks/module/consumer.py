"""
🟡 ORCHESTRATOR-TASKS consumer — получает статусы от data-channel
и обновляет кэш для GET /robot_status.
Фильтрует: обновляет статус ТОЛЬКО при получении mission_status.
"""
import os
import json
import threading
import multiprocessing

from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver
from .api import update_status


MODULE_NAME: str = os.getenv("MODULE_NAME")
_response_queue: multiprocessing.Queue = None


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    print(f"[{MODULE_NAME}] received from {src}: {operation}")

    # Обновляем кэш ТОЛЬКО для mission_status (не для alert/dock_event/mission_rejected)
    if operation == "mission_status" and isinstance(data, dict):
        update_status({"status": "mission_status", "info": data})

    if _response_queue is not None:
        _response_queue.put(details)


def consumer_job(args, config, response_queue):
    global _response_queue
    _response_queue = response_queue

    consumer = Consumer(config)

    def reset_offset(verifier_consumer, partitions):
        if not args.reset:
            return
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        verifier_consumer.assign(partitions)

    topic = MODULE_NAME
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[error] {msg.error()}")
                continue
            try:
                id = msg.key().decode("utf-8")
                details_str = msg.value().decode("utf-8")
                handle_event(id, details_str)
            except Exception as e:
                print(f"[error] {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config, response_queue):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(
        target=lambda: consumer_job(args, config, response_queue),
        daemon=True,
    ).start()
