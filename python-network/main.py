import time
import threading
from broker import BrokerNode
from subscriber import Subscriber

if __name__ == "__main__":
    broker_addresses = {
        "broker_0": ("localhost", 8001),
        "broker_1": ("localhost", 8002),
        "broker_2": ("localhost", 8003),
    }

    b2 = BrokerNode("broker_2", "localhost", 8003,
                    all_brokers=broker_addresses)
    b1 = BrokerNode("broker_1", "localhost", 8002,
                    next_broker_host="localhost", next_broker_port=8003,
                    all_brokers=broker_addresses)
    b0 = BrokerNode("broker_0", "localhost", 8001,
                    next_broker_host="localhost", next_broker_port=8002,
                    all_brokers=broker_addresses)

    b2.start()
    b1.start()
    b0.start()

    b0.start_kafka_ingestion(topic_name='raw-publications')
    time.sleep(2)

    sub = Subscriber("client_1", broker_addresses)
    sub.connect_to_brokers()
    sub.load_and_route_subscriptions("subscriptions.txt")

    sub2 = Subscriber("client_2", broker_addresses)
    sub2.connect_to_brokers()
    sub2.load_and_route_subscriptions("subscriptions.txt")

    sub3 = Subscriber("client_3", broker_addresses)
    sub3.connect_to_brokers()
    sub3.load_and_route_subscriptions("subscriptions.txt")

    print("\nSistemul ruleaza. Acum poti porni aplicatia Java...")

    # def simulate_failure():
    #     time.sleep(30)
    #     print("\n[SIMULARE] broker_0 cade acum!")
    #     b0.running = False
    #     try:
    #         b0.server_socket.close()
    #     except Exception:
    #         pass
    #     print("[SIMULARE] broker_0 oprit. broker_1 preia ingestia din Kafka...")
    #     b1.start_kafka_ingestion(topic_name='raw-publications')
    #
    # threading.Thread(target=simulate_failure, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSistem inchis.")