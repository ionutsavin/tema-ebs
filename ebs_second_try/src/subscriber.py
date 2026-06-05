"""
Nod subscriber care se conectează aleatoriu la brokeri pentru a înregistra subscripții
"""

import socket
import json
import random
import threading
from typing import Dict, Tuple


class Subscriber:

    def __init__(self, subscriber_id: str, broker_addresses: Dict[str, Tuple[str, int]]):
        self.subscriber_id = subscriber_id
        self.broker_addresses = broker_addresses
        self.subscriptions = []
        self.lock = threading.Lock()

    def subscribe(self, subscription: Dict) -> Dict:
        # Alege un broker aleatoriu
        broker_id = random.choice(list(self.broker_addresses.keys()))
        host, port = self.broker_addresses[broker_id]

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))

            message = {
                'type': 'subscribe',
                'subscriber_id': self.subscriber_id,
                'subscription': subscription
            }

            sock.send(json.dumps(message).encode('utf-8'))
            response = json.loads(sock.recv(4096).decode('utf-8'))
            sock.close()

            with self.lock:
                self.subscriptions.append(subscription)

            return response

        except Exception as e:
            print(f"  Subscriber {self.subscriber_id} eroare: {e}")
            return {'status': 'error', 'message': str(e)}

    def get_stats(self) -> Dict:
        with self.lock:
            return {
                'subscriber_id': self.subscriber_id,
                'subscriptions_count': len(self.subscriptions)
            }