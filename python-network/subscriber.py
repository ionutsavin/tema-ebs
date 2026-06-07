import socket
import json
import threading
import time
import argparse
from typing import Dict, Tuple
from consistent_hash import ConsistentHashRing
from utils import parse_java_subscription, encrypt_subscription


class Subscriber:
    def __init__(self, subscriber_id: str, broker_addresses: Dict[str, Tuple[str, int]]):
        self.subscriber_id = subscriber_id
        self.broker_addresses = broker_addresses

        self.hash_ring = ConsistentHashRing(list(broker_addresses.keys()))
        self.sockets = {}

        self.seen_ts = set()
        self.seen_lock = threading.Lock()
        self._sub_counter = 0

    def connect_to_brokers(self):
        for broker_id, (host, port) in self.broker_addresses.items():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                self.sockets[broker_id] = sock

                # Register subscriber ID with this broker to receive match notifications
                register_msg = {'type': 'register', 'subscriber_id': self.subscriber_id}
                sock.sendall((json.dumps(register_msg) + '\n').encode('utf-8'))

                threading.Thread(target=self._listen_for_matches, args=(broker_id, sock), daemon=True).start()
            except Exception:
                print(f"[Subscriber {self.subscriber_id}] Could not connect to {broker_id}")

    def load_and_route_subscriptions(self, filepath: str):
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    sub_str = line.strip()
                    if not sub_str:
                        continue

                    sub_dict = parse_java_subscription(sub_str)
                    if not sub_dict:
                        continue

                    # encrypt subscription before sending
                    encrypted_sub = encrypt_subscription(sub_dict)
                    if not encrypted_sub:
                        continue

                    target_broker = self.hash_ring.get_node(
                        f"{self.subscriber_id}:{self._sub_counter}"
                    )
                    self._sub_counter += 1

                    if target_broker in self.sockets:
                        message = {
                            'type': 'subscribe',
                            'subscriber_id': self.subscriber_id,
                            'subscription': encrypted_sub,
                        }
                        self.sockets[target_broker].sendall(
                            (json.dumps(message) + '\n').encode('utf-8')
                        )

            print(f"[Subscriber {self.subscriber_id}] Subscriptions routed successfully.")
        except FileNotFoundError:
            print("Subscriptions file not found.")

    def _listen_for_matches(self, broker_id: str, sock: socket.socket):
        buffer = ""
        try:
            while True:
                data = sock.recv(65536).decode('utf-8')
                if not data:
                    break

                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if not line.strip():
                        continue

                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get('type') == 'match':
                        pub = msg['publication']
                        ts = pub.get('_ts')

                        if ts is not None:
                            with self.seen_lock:
                                if ts in self.seen_ts:
                                    continue
                                self.seen_ts.add(ts)
                                if len(self.seen_ts) > 100000:
                                    self.seen_ts.clear()

                        print(f"[{self.subscriber_id}] MATCH received from {broker_id}: {pub}")
        except Exception:
            pass


BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}


def main():
    parser = argparse.ArgumentParser(description="Start a subscriber node")
    parser.add_argument("--id", required=True, choices=["client_1", "client_2", "client_3"])
    parser.add_argument("--subscriptions", default="subscriptions.txt")
    args = parser.parse_args()

    sub = Subscriber(args.id, BROKER_ADDRESSES)
    sub.connect_to_brokers()
    time.sleep(2)
    sub.load_and_route_subscriptions(args.subscriptions)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{args.id}] Shutting down.")


if __name__ == "__main__":
    main()