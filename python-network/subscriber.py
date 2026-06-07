import socket
import json
import threading
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

                # Inregistram ID-ul subscriberului la acest broker pentru a putea primi match-uri
                register_msg = {'type': 'register', 'subscriber_id': self.subscriber_id}
                sock.sendall((json.dumps(register_msg) + '\n').encode('utf-8'))

                threading.Thread(target=self._listen_for_matches, args=(broker_id, sock), daemon=True).start()
            except Exception:
                print(f"[Subscriber {self.subscriber_id}] Nu m-am putut conecta la {broker_id}")

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

                    # criptam subscriptia inainte sa o trimitem
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

            print(f"[Subscriber {self.subscriber_id}] Subscriptiile au fost rutate cu succes.")
        except FileNotFoundError:
            print("Fisierul cu subscriptii nu a fost gasit.")

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

                        print(f"[{self.subscriber_id}] MATCH primit de la {broker_id}: {pub}")
        except Exception:
            pass