"""
Nod broker individual din rețea - stochează subscripții și face matching
"""

import socket
import json
import threading
import time
import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple
from matching_engine import MatchingEngine
from consistent_hash import ConsistentHashRing


class BrokerNode:

    def __init__(self, broker_id: str, host: str = 'localhost', port: int = 0):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.subscriptions: Dict[str, List[Dict]] = defaultdict(list)
        self.matching_engine = MatchingEngine()
        self.hash_ring: ConsistentHashRing = None
        self.peers: Dict[str, Tuple[str, int]] = {}
        self.running = True
        self.server_socket = None
        self.messages_processed = 0
        self.lock = threading.Lock()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        self.port = self.server_socket.getsockname()[1]
        print(f"  Broker {self.broker_id} pornit pe portul {self.port}")

        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()

    def _accept_connections(self):
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                )
                client_thread.start()
            except Exception:
                if self.running:
                    pass

    def _handle_client(self, client_socket: socket.socket):
        try:
            data = client_socket.recv(65536).decode('utf-8')
            if not data:
                return

            message = json.loads(data)
            msg_type = message.get('type')

            if msg_type == 'subscribe':
                self._handle_subscription(message, client_socket)
            elif msg_type == 'publication':
                self._handle_publication(message, client_socket)

        except Exception as e:
            print(f"  Broker {self.broker_id} eroare: {e}")
        finally:
            client_socket.close()

    def _handle_subscription(self, message: Dict, client_socket: socket.socket):
        """Procesează cerere de abonare cu rutare distribuită"""
        subscriber_id = message['subscriber_id']
        subscription = message['subscription']

        # Calculează hash pentru subscripție
        sub_hash = hashlib.md5(json.dumps(subscription, sort_keys=True).encode()).hexdigest()

        # Rutare avansată: găsește broker-ul responsabil
        if self.hash_ring:
            responsible_broker = self.hash_ring.get_node(sub_hash)

            if responsible_broker != self.broker_id:
                response = {
                    'status': 'forwarded',
                    'broker': responsible_broker
                }
                client_socket.send(json.dumps(response).encode('utf-8'))
                return

        # Stochează local subscripția
        with self.lock:
            self.subscriptions[subscriber_id].append({
                'hash': sub_hash,
                'subscription': subscription,
                'timestamp': time.time()
            })

        response = {
            'status': 'subscribed',
            'broker': self.broker_id
        }
        client_socket.send(json.dumps(response).encode('utf-8'))

    def _handle_publication(self, message: Dict, client_socket: socket.socket):
        publication = message['publication']

        matched_subscribers = []
        with self.lock:
            for subscriber_id, subs in list(self.subscriptions.items()):
                for sub_info in subs:
                    if self.matching_engine.matches(publication, sub_info['subscription']):
                        matched_subscribers.append(subscriber_id)

            self.messages_processed += len(matched_subscribers)

        response = {
            'status': 'processed',
            'matches': len(matched_subscribers),
            'broker': self.broker_id
        }
        client_socket.send(json.dumps(response).encode('utf-8'))

    def setup_peers(self, peer_addresses: Dict[str, Tuple[str, int]]):
        """Configurează conexiunile cu alți brokeri din rețea"""
        self.peers = peer_addresses
        broker_list = list(peer_addresses.keys()) + [self.broker_id]
        self.hash_ring = ConsistentHashRing(broker_list)

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()

    def get_stats(self) -> Dict:
        with self.lock:
            total_subscriptions = sum(len(subs) for subs in self.subscriptions.values())
            return {
                'broker_id': self.broker_id,
                'subscriptions': total_subscriptions,
                'messages_processed': self.messages_processed
            }


class BrokerNetwork:
    """brokeri interconectați (overlay network)"""

    def __init__(self, num_brokers: int = 3):
        self.brokers: Dict[str, BrokerNode] = {}
        self.addresses: Dict[str, Tuple[str, int]] = {}

        print(f"\nInitializare retea cu {num_brokers} brokeri...")

        for i in range(num_brokers):
            broker = BrokerNode(f"broker_{i}")
            broker.start()
            self.brokers[f"broker_{i}"] = broker
            self.addresses[f"broker_{i}"] = ('localhost', broker.port)

        # Conectează brokerii între ei
        for broker_id, broker in self.brokers.items():
            peers = {bid: addr for bid, addr in self.addresses.items() if bid != broker_id}
            broker.setup_peers(peers)

        print(f"Retea creata cu succes!\n")

    def get_broker_addresses(self) -> Dict[str, Tuple[str, int]]:
        return self.addresses

    def stop_all(self):
        for broker in self.brokers.values():
            broker.stop()

    def get_stats(self) -> List[Dict]:
        return [broker.get_stats() for broker in self.brokers.values()]