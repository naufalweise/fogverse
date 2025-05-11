# from kafka import KafkaProducer, KafkaConsumer
# import time

# TOPIC = "test_topic"

# def produce():
#     producer = KafkaProducer(bootstrap_servers="localhost:29092")
#     while True:
#         message = f"hello-{int(time.time())}"
#         producer.send(TOPIC, message.encode("utf-8"))
#         print(f"Produced: {message}")
#         time.sleep(2)

# def consume():
#     consumer = KafkaConsumer(TOPIC, bootstrap_servers="localhost:29092", auto_offset_reset="latest")
#     for msg in consumer:
#         print(f"Consumed: {msg.value.decode('utf-8')}")

# if __name__ == "__main__":
#     from multiprocessing import Process
#     p1 = Process(target=produce)
#     p2 = Process(target=consume)

#     p1.start()
#     p2.start()
#     p1.join()
#     p2.join()
