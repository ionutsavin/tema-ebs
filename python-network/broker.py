import socket
import json
import threading
import time
import argparse
from typing import Dict
from matching_engine import MatchingEngine
from kafka import KafkaConsumer
import base64
import publication_pb2


class BrokerNode:
    def __init__(self, broker_id: str, host: str, port: int,
                 next_broker_host: str = None, next_broker_port: int = None,
                 all_brokers: Dict[str, tuple] = None):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.next_broker = (next_broker_host, next_broker_port) if next_broker_host else None
        self.all_brokers = all_brokers or {}

        self.subscriptions = {}
        self.replicated_subs = {}
        self.subscriber_sockets = {}
        self.matching_engine = MatchingEngine()

        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()

        self.processed_pub_count = 0
        self.sent_match_count = 0

        self.broker_status = {bid: True for bid in self.all_brokers}

        self.next_broker_socket = None
        self.next_broker_lock = threading.Lock()

        threading.Thread(target=self._health_check_loop, daemon=True).start()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        print(f"[{self.broker_id}] Started on port {self.port}", flush=True)
        threading.Thread(target=self._accept_connections, daemon=True).start()

    def _accept_connections(self):
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
            except Exception:
                pass

    def _handle_client(self, client_socket: socket.socket):
        buffer = ""
        try:
            while self.running:
                data = client_socket.recv(65536).decode('utf-8')
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if message.get('type') == 'subscribe':
                        self._handle_subscription(message, client_socket)
                    elif message.get('type') == 'register':
                        self._handle_register(message, client_socket)
                    elif message.get('type') == 'subscribe_replicated':
                        self._handle_replicated_subscription(message)
                    elif message.get('type') == 'publication':
                        self._handle_publication(message)
        except Exception as e:
            print(f"[{self.broker_id}] Socket error: {e}")
        finally:
            client_socket.close()

    def _handle_subscription(self, message: Dict, client_socket: socket.socket):
        subscriber_id = message['subscriber_id']
        raw_sub = message['subscription']

        subscription = {
            field: tuple(cond) if isinstance(cond, list) else cond
            for field, cond in raw_sub.items()
        }

        with self.lock:
            if subscriber_id not in self.subscriptions:
                self.subscriptions[subscriber_id] = []
            self.subscriptions[subscriber_id].append(subscription)
            self.subscriber_sockets[subscriber_id] = client_socket

        if self.next_broker:
            rep_message = {
                'type': 'subscribe_replicated',
                'subscriber_id': subscriber_id,
                'subscription': message['subscription'],
                'owner_broker': self.broker_id,
            }
            self._forward_message(rep_message, self.next_broker)

    def _handle_register(self, message: Dict, client_socket: socket.socket):
        subscriber_id = message['subscriber_id']
        with self.lock:
            self.subscriber_sockets[subscriber_id] = client_socket
        print(f"[{self.broker_id}] Subscriber {subscriber_id} registered.")

    def _handle_replicated_subscription(self, message: Dict):
        subscriber_id = message['subscriber_id']
        raw_sub = message['subscription']

        subscription = {
            field: tuple(cond) if isinstance(cond, list) else cond
            for field, cond in raw_sub.items()
        }

        with self.lock:
            if subscriber_id not in self.replicated_subs:
                self.replicated_subs[subscriber_id] = []
            self.replicated_subs[subscriber_id].append({
                'subscription': subscription,
                'owner_broker': message['owner_broker'],
            })

    def _handle_publication(self, message: Dict):
        pub_data = message['publication']

        # broker only sees hashes - demonstrates bonus 3
        if not hasattr(self, '_crypto_debug'):
            self._crypto_debug = True
            print(f"[{self.broker_id}] Encrypted publication (broker view): {pub_data}")

        with self.lock:
            for sub_id, subs in self.subscriptions.items():
                for sub in subs:
                    if self.matching_engine.matches(pub_data, sub):
                        self._notify_subscriber(sub_id, pub_data)
                        break

            for sub_id, rep_subs in self.replicated_subs.items():
                for rep in rep_subs:
                    owner = rep['owner_broker']
                    if not self.broker_status.get(owner, True):
                        if self.matching_engine.matches(pub_data, rep['subscription']):
                            print(f"[{self.broker_id}] Taking over match for {sub_id} — owner {owner} is offline")
                            self._notify_subscriber(sub_id, pub_data)
                            break

        if self.next_broker:
            # DEBUG: print(f"[{self.broker_id}] Forwarding to next broker.")
            self._forward_publication(message)

    def _notify_subscriber(self, sub_id: str, pub_data: Dict):
        if sub_id in self.subscriber_sockets:
            try:
                notification = json.dumps({'type': 'match', 'publication': pub_data}) + '\n'
                self.subscriber_sockets[sub_id].sendall(notification.encode('utf-8'))
            except Exception:
                pass

    def _get_next_broker_socket(self):
        with self.next_broker_lock:
            if self.next_broker_socket is None:
                try:
                    s = socket.socket()
                    s.settimeout(2)
                    s.connect(self.next_broker)
                    self.next_broker_socket = s
                except Exception:
                    return None
            return self.next_broker_socket

    def _forward_publication(self, message: Dict):
        if not self.next_broker:
            return
        try:
            sock = self._get_next_broker_socket()
            if sock:
                sock.sendall((json.dumps(message) + '\n').encode())
                return
        except Exception:
            with self.next_broker_lock:
                self.next_broker_socket = None
            for bid, addr in self.all_brokers.items():
                if addr == self.next_broker:
                    if self.broker_status.get(bid, True):
                        print(f"[{self.broker_id}] Next broker {bid} unreachable, marked offline.")
                        self._mark_broker_down(self.next_broker)
                    break

    def _forward_message(self, message: Dict, target: tuple):
        try:
            s = socket.socket()
            s.settimeout(2)
            s.connect(target)
            s.sendall((json.dumps(message) + '\n').encode())
            s.close()
        except Exception:
            pass

    def _mark_broker_down(self, address: tuple):
        for bid, addr in self.all_brokers.items():
            if addr == address:
                with self.lock:
                    self.broker_status[bid] = False
                print(f"[{self.broker_id}] {bid} marked as offline.")
                break

    def _health_check_loop(self):
        while True:
            time.sleep(5)
            for bid, addr in self.all_brokers.items():
                if bid == self.broker_id:
                    continue
                try:
                    s = socket.socket()
                    s.settimeout(1)
                    s.connect(addr)
                    s.close()
                    with self.lock:
                        if not self.broker_status.get(bid, True):
                            print(f"[{self.broker_id}] {bid} came back online.")
                        self.broker_status[bid] = True
                except Exception:
                    with self.lock:
                        if self.broker_status.get(bid, True):
                            print(f"[{self.broker_id}] {bid} detected offline via health check.")
                        self.broker_status[bid] = False

    def start_kafka_ingestion(self, topic_name='raw-publications', bootstrap_servers='localhost:9092'):
        def consume():
            while True:
                try:
                    consumer = KafkaConsumer(
                        topic_name,
                        bootstrap_servers=[bootstrap_servers],
                        auto_offset_reset='latest',
                        value_deserializer=lambda m: m.decode('utf-8', errors='ignore')
                    )
                    break
                except Exception as e:
                    print(f"[KAFKA] Waiting for Kafka... ({e})", flush=True)
                    time.sleep(2)
            print(f"[{self.broker_id}] Connected to Kafka. Waiting for publications...", flush=True)
            try:
                for count, message in enumerate(consumer):
                    raw_str = message.value
                    if count % 10000 == 0:
                        print(f"[KAFKA DEBUG] Packet {count}: {raw_str}", flush=True)

                    proto = publication_pb2.Publication()
                    proto.ParseFromString(base64.b64decode(raw_str))
                    pub_dict = {
                        "company": proto.company,
                        "value": proto.value,
                        "drop": proto.drop,
                        "variation": proto.variation,
                        "_ts": proto._ts,
                    }

                    self._handle_publication({'type': 'publication', 'publication': pub_dict})
            except Exception as e:
                print(f"[KAFKA ERROR] {e}", flush=True)

        threading.Thread(target=consume, daemon=True).start()


BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}

NEXT_BROKER = {
    "broker_0": ("localhost", 8002),
    "broker_1": ("localhost", 8003),
    "broker_2": None,
}

KAFKA_TOPIC = "raw-publications"


def main():
    parser = argparse.ArgumentParser(description="Start a broker node")
    parser.add_argument("--id", required=True, choices=list(BROKER_ADDRESSES.keys()))
    args = parser.parse_args()

    bid = args.id
    host, port = BROKER_ADDRESSES[bid]
    nb = NEXT_BROKER[bid]
    next_host = nb[0] if nb else None
    next_port = nb[1] if nb else None

    broker = BrokerNode(bid, host, port,
                        next_broker_host=next_host, next_broker_port=next_port,
                        all_brokers=BROKER_ADDRESSES)
    broker.start()

    if bid == "broker_0":
        broker.start_kafka_ingestion(topic_name=KAFKA_TOPIC)
        print(f"[{bid}] Consuming from Kafka topic '{KAFKA_TOPIC}'...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{bid}] Shutting down.")
        broker.running = False


if __name__ == "__main__":
    main()