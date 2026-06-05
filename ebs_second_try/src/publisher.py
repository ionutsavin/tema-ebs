"""
Nod publisher care emite publicații către rețeaua de brokeri
"""

import socket
import json
import random
import time
import threading
from typing import Dict, Tuple


class Publisher:

    def __init__(self, publisher_id: str, broker_addresses: Dict[str, Tuple[str, int]]):
        self.publisher_id = publisher_id
        self.broker_addresses = broker_addresses
        self.published_count = 0
        self.lock = threading.Lock()

    def publish(self, publication: Dict) -> Tuple[bool, float]:
        """Publică un mesaj la un broker ales aleatoriu"""
        broker_id = random.choice(list(self.broker_addresses.keys()))
        host, port = self.broker_addresses[broker_id]

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))

            message = {
                'type': 'publication',
                'publication': publication
            }

            start_time = time.time()
            sock.send(json.dumps(message).encode('utf-8'))
            response = json.loads(sock.recv(4096).decode('utf-8'))
            end_time = time.time()

            sock.close()

            latency = (end_time - start_time) * 1000  # milisecunde

            with self.lock:
                self.published_count += 1

            return True, latency

        except Exception as e:
            print(f"  Publisher {self.publisher_id} eroare: {e}")
            return False, 0

    def get_stats(self) -> Dict:
        with self.lock:
            return {'published_count': self.published_count}