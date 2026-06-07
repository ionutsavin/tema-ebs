import socket
import json
import threading
from typing import Dict, Tuple
from consistent_hash import ConsistentHashRing
from utils import parse_java_subscription


class Subscriber:
    def __init__(self, subscriber_id: str, broker_addresses: Dict[str, Tuple[str, int]]):
        self.subscriber_id = subscriber_id
        self.broker_addresses = broker_addresses

        self.hash_ring = ConsistentHashRing(list(broker_addresses.keys()))
        self.sockets = {}

        self.seen_ts = set()
        self.seen_lock = threading.Lock()

    def connect_to_brokers(self):
        for broker_id, (host, port) in self.broker_addresses.items():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                self.sockets[broker_id] = sock
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

                    first_key = list(sub_dict.keys())[0]
                    target_broker = self.hash_ring.get_node(first_key)

                    if target_broker in self.sockets:
                        message = {
                            'type': 'subscribe',
                            'subscriber_id': self.subscriber_id,
                            'subscription': sub_dict
                        }
                        self.sockets[target_broker].send((json.dumps(message) + '\n').encode('utf-8'))

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

                    msg = json.loads(line)
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