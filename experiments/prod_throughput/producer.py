import time
import threading
import random
from confluent_kafka import Producer, Consumer, KafkaError

from experiments.constants import BROKER_ADDRESS, TOPIC_NAME

TARGET_TP_MB = 5  # Target production rate in MB/s
TARGET_TC = True  # Whether consumer is active

produce_count = 0
produce_bytes = 0
consume_count = 0
consume_bytes = 0

produce_lock = threading.Lock()
consume_lock = threading.Lock()


def delivery_report(err, msg):
    global produce_bytes, produce_count
    if err is None:
        with produce_lock:
            produce_bytes += len(msg.value())
            produce_count += 1


def produce_loop():
    global produce_bytes
    p = Producer({'bootstrap.servers': BROKER_ADDRESS})
    msg_size = 10000  # 10KB message
    interval = (msg_size / 1024**2) / TARGET_TP_MB  # time between messages to meet rate

    while True:
        value = bytes(random.getrandbits(8) for _ in range(msg_size))
        p.produce(TOPIC_NAME, value=value, callback=delivery_report)
        p.poll(0)
        time.sleep(interval)  # attempt rate limiter


def consume_loop():
    global consume_bytes, consume_count
    c = Consumer({
        'bootstrap.servers': BROKER_ADDRESS,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([TOPIC_NAME])

    while True:
        msg = c.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        with consume_lock:
            consume_bytes += len(msg.value())
            consume_count += 1


def monitor():
    global produce_bytes, consume_bytes
    while True:
        time.sleep(1)
        with produce_lock:
            tp = produce_bytes / (1024**2)
            produce_bytes = 0
        with consume_lock:
            tc = consume_bytes / (1024**2)
            consume_bytes = 0
        print(f"Actual Tp: {tp:.2f} MB/s | Actual Tc: {tc:.2f} MB/s")


if __name__ == "__main__":
    threads = []

    threads.append(threading.Thread(target=produce_loop))
    if TARGET_TC:
        threads.append(threading.Thread(target=consume_loop))
    threads.append(threading.Thread(target=monitor))

    for t in threads:
        t.daemon = True
        t.start()

    while True:
        time.sleep(10)
