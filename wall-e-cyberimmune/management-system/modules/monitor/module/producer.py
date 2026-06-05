"""
Monitor producer — после проверки политики, пересылает сообщение
в топик-получатель (deliver_to).
"""
import os
import json
import threading
import multiprocessing

from confluent_kafka import Producer


_requests_queue: multiprocessing.Queue = None
MODULE_NAME = os.getenv("MODULE_NAME")


def proceed_to_deliver(id, details):
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print(f"[error] Message failed delivery: {err}")

    while True:
        event_details = requests_queue.get()
        topic = event_details["deliver_to"]
        try:
            producer.produce(
                topic,
                json.dumps(event_details),
                event_details["id"],
                callback=delivery_callback,
            )
            producer.poll(1)
            producer.flush()
        except Exception as e:
            print(f"[error] producer: {e}")


def start_producer(args, config, requests_queue):
    print(f"{MODULE_NAME}_producer started")
    global _requests_queue
    _requests_queue = requests_queue
    threading.Thread(
        target=lambda: producer_job(args, config, requests_queue),
        daemon=True,
    ).start()
