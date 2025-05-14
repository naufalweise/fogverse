import threading
import time
import random
from confluent_kafka import Producer, Consumer

from experiments.constants import BROKER_ADDRESS, MESSAGE_SIZE, TOPIC_NAME

produce_count = 0
produce_bytes = 0
consume_count = 0
consume_bytes = 0

produce_lock = threading.Lock()
consume_lock = threading.Lock()
cancel_signal = threading.Event()

target_tp_mb = 20
load_steps = []
actual_tp = []
actual_tc = []

stop_all = threading.Event()


def delivery_report(err, msg):
    global produce_bytes, produce_count
    if err is None:
        with produce_lock:
            produce_bytes += len(msg.value())
            produce_count += 1


def produce_loop():
    global target_tp_mb
    p = Producer({'bootstrap.servers': BROKER_ADDRESS})
    while not stop_all.is_set():
        current_tp = target_tp_mb
        interval = (MESSAGE_SIZE / 1024**2) / current_tp
        value = bytes(random.getrandbits(8) for _ in range(MESSAGE_SIZE))
        p.produce(TOPIC_NAME, value=value, callback=delivery_report)
        p.poll(0)
        time.sleep(interval)


def consume_loop():
    global consume_bytes, consume_count
    c = Consumer({
        'bootstrap.servers': BROKER_ADDRESS,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([TOPIC_NAME])
    while not stop_all.is_set():
        msg = c.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        with consume_lock:
            consume_bytes += len(msg.value())
            consume_count += 1


def monitor_loop():
    global produce_bytes, consume_bytes
    while not stop_all.is_set():
        time.sleep(1)
        timestamp = time.time()
        with produce_lock:
            tp = produce_bytes / (1024**2)
            produce_bytes = 0
        with consume_lock:
            tc = consume_bytes / (1024**2)
            consume_bytes = 0
        actual_tp.append((timestamp, tp))
        actual_tc.append((timestamp, tc))


def increase_tp_loop():
    global target_tp_mb
    while not cancel_signal.is_set():
        timestamp = time.time()
        load_steps.append((timestamp, target_tp_mb))
        time.sleep(8)
        target_tp_mb += 4
    # Final step timestamp when limit reached.
    load_steps.append((time.time(), target_tp_mb))


def stop_client_data_flow(): cancel_signal.set()


def run_throughput_test():
    threads = [
        threading.Thread(target=produce_loop),
        threading.Thread(target=consume_loop),
        threading.Thread(target=monitor_loop),
        threading.Thread(target=increase_tp_loop)
    ]

    for t in threads:
        t.start()

    while not cancel_signal.is_set():
        time.sleep(0.5)  # Wait until signal externally set.

    stop_all.set()
    for t in threads:
        t.join(timeout=2)

    if len(load_steps) < 2:
        return 0.0, 0.0  # Not enough data to analyze second-last period.

    # Get second-last time window.
    t_start, _ = load_steps[-2]
    t_end, _ = load_steps[-1]

    tp_vals = [v for ts, v in actual_tp if t_start <= ts < t_end]
    tc_vals = [v for ts, v in actual_tc if t_start <= ts < t_end]

    avg_tp = sum(tp_vals) / len(tp_vals) if tp_vals else 0.0
    avg_tc = sum(tc_vals) / len(tc_vals) if tc_vals else 0.0

    return avg_tp, avg_tc
