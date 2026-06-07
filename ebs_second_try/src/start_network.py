import time
from broker import BrokerNode
from subscriber import Subscriber

if __name__ == "__main__":
    # 1. Definim topologia (Pipeline liniar 0 -> 1 -> 2)
    b2 = BrokerNode("broker_2", "localhost", 8003)
    b1 = BrokerNode("broker_1", "localhost", 8002, next_broker_host="localhost", next_broker_port=8003)
    b0 = BrokerNode("broker_0", "localhost", 8001, next_broker_host="localhost", next_broker_port=8002)

    # 2. Pornim brokerii in ordine inversa
    b2.start()
    b1.start()
    b0.start()

    # 3. Pornim ingerarea din Kafka DOAR pe primul broker
    # Asigura-te ca Docker-ul cu Kafka este pornit inainte sa rulezi asta
    b0.start_kafka_ingestion(topic_name='raw-publications')

    time.sleep(2)  # Asteptam sa se deschida socket-urile

    # 4. Conectam Subscriberul si incarcam datele
    broker_addresses = {
        "broker_0": ("localhost", 8001),
        "broker_1": ("localhost", 8002),
        "broker_2": ("localhost", 8003)
    }

    sub1 = Subscriber("client_1", broker_addresses)
    sub1.connect_to_brokers()
    sub1.load_and_route_subscriptions("subscriptions.txt")
    
    sub2 = Subscriber("client_2", broker_addresses)
    sub2.connect_to_brokers()
    sub2.load_and_route_subscriptions("subscriptions.txt")

    sub3 = Subscriber("client_3", broker_addresses)
    sub3.connect_to_brokers()
    sub3.load_and_route_subscriptions("subscriptions.txt")

    print("\nSistemul ruleaza. Acum poti porni aplicatia Java pentru a pompa date in Kafka...")

    # Mentine scriptul activ
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSistem inchis.")