import socket
import json
import threading
from typing import Dict, Tuple
from matching_engine import MatchingEngine
from kafka import KafkaConsumer
from utils import parse_java_publication


class BrokerNode:
    def __init__(self, broker_id: str, host: str, port: int, next_broker_host: str = None,
                 next_broker_port: int = None):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.next_broker = (next_broker_host, next_broker_port) if next_broker_host else None

        self.subscriptions = {}  # subscriber_id -> lista de subscriptii
        self.subscriber_sockets = {}  # subscriber_id -> socket_activ
        self.matching_engine = MatchingEngine()

        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        print(f"[{self.broker_id}] Pornit pe portul {self.port}")

        threading.Thread(target=self._accept_connections, daemon=True).start()

    def _accept_connections(self):
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
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

                    message = json.loads(line)
                    if message.get('type') == 'subscribe':
                        self._handle_subscription(message, client_socket)
                    elif message.get('type') == 'publication':
                        self._handle_publication(message)
        except Exception as e:
            print(f"[{self.broker_id}] Eroare socket intern: {e}")  # Am scos pass-ul si afisam eroarea
        finally:
            client_socket.close()

    def _handle_subscription(self, message: Dict, client_socket: socket.socket):
        subscriber_id = message['subscriber_id']
        raw_sub = message['subscription']

        # JSON convertește tuplurile Python în liste — le reconvertim înapoi
        subscription = {
            field: tuple(cond) if isinstance(cond, list) else cond
            for field, cond in raw_sub.items()
        }

        with self.lock:
            if subscriber_id not in self.subscriptions:
                self.subscriptions[subscriber_id] = []
            self.subscriptions[subscriber_id].append(subscription)
            self.subscriber_sockets[subscriber_id] = client_socket

    def _handle_publication(self, message: Dict):
        pub_data = message['publication']
        matched = False

        with self.lock:
            for sub_id, subs in self.subscriptions.items():
                for sub in subs:
                    if self.matching_engine.matches(pub_data, sub):
                        matched = True
                        if sub_id in self.subscriber_sockets:
                            try:
                                notification = json.dumps({'type': 'match', 'publication': pub_data}) + '\n'
                                self.subscriber_sockets[sub_id].send(notification.encode('utf-8'))
                            except Exception:
                                pass  # Socket-ul clientului a cazut
                        break  # Evitam trimiterea duplicata catre acelasi client pentru aceeasi publicatie

        # Forward catre urmatorul broker din topologie
        if self.next_broker:
            self._forward_publication(message)

    def _forward_publication(self, message: Dict):
        try:
            forward_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            forward_socket.connect(self.next_broker)
            payload = json.dumps(message) + '\n'
            forward_socket.send(payload.encode('utf-8'))
            forward_socket.close()
        except Exception as e:
            pass

    def start_kafka_ingestion(self, topic_name='raw-publications', bootstrap_servers='localhost:9092'):
        def consume():
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[bootstrap_servers],
                auto_offset_reset='latest',
                value_deserializer=lambda m: m.decode('utf-8', errors='ignore')
            )
            print(f"[{self.broker_id}] Conectat la Kafka. Asteapta publicatii...")

            try:
                for count, message in enumerate(consumer):
                    raw_str = message.value

                    # Printam din 100 in 100 ca sa nu blocam consola, dar sa vedem ca vin date
                    if count % 10000 == 0:
                        print(f"[KAFKA DEBUG] Am primit pachetul {count} din Java: {raw_str}")

                    pub_dict = parse_java_publication(raw_str)
                    self._handle_publication({'type': 'publication', 'publication': pub_dict})
            except Exception as e:
                print(f"[EROARE CRITICA KAFKA] Ceva a crapat la citire: {e}")

        threading.Thread(target=consume, daemon=True).start()