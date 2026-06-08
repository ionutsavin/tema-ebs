from kafka import KafkaProducer, KafkaConsumer
import time
import threading

def consume():
    print("Consumer starting...")
    consumer = KafkaConsumer(
        'test-topic-2',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        api_version=(2, 8, 1) # trying to force api version
    )
    for msg in consumer:
        print(f"Consumed: {msg.value}")
    print("Consumer done.")

def produce():
    print("Producer starting...")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(2, 8, 1))
    for i in range(5):
        producer.send('test-topic-2', b'test message ' + str(i).encode())
        print(f"Produced message {i}")
        time.sleep(0.5)
    producer.flush()
    producer.close()
    print("Producer done.")

t1 = threading.Thread(target=consume)
t1.start()
time.sleep(2)
t2 = threading.Thread(target=produce)
t2.start()

t1.join()
t2.join()
