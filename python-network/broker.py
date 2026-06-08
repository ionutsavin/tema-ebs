import socket
import json
import threading
import time
import argparse
import hashlib
import logging
import signal
import sys
from typing import Dict, Optional
from matching_engine import MatchingEngine
from confluent_kafka import Consumer, KafkaError
from consistent_hash import ConsistentHashRing
from system_logger import setup_logger, stop_logger
import base64
import publication_pb2


class BrokerNode:
    def __init__(
        self,
        broker_id: str,
        host: str,
        port: int,
        next_broker_host: str = None,
        next_broker_port: int = None,
        all_brokers: Dict[str, tuple] = None,
        logger: logging.Logger = None,
    ):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.next_broker = (
            (next_broker_host, next_broker_port) if next_broker_host else None
        )
        self.all_brokers = all_brokers or {}
        self.logger = logger

        self.hash_ring = ConsistentHashRing(list(self.all_brokers.keys()))

        # Active subscriptions owned by THIS broker (matching happens here)
        self.subscriptions = {}
        # Replicated subscriptions from another broker for failover
        self.replicated_subs = {}
        # Direct TCP sockets to subscribers that connected to THIS broker
        self.subscriber_sockets = {}
        self.matching_engine = MatchingEngine()

        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()

        self.processed_pub_count = 0
        self.sent_match_count = 0
        self._logged_sample = False

        self.broker_status = {bid: True for bid in self.all_brokers}

        self.next_broker_socket = None
        self.next_broker_lock = threading.Lock()
        self.entry_broker_sockets: Dict[str, Optional[socket.socket]] = {}
        self.entry_broker_lock = threading.Lock()

        threading.Thread(target=self._health_check_loop, daemon=True).start()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        self.logger.info("started port=%d", self.port)
        threading.Thread(target=self._accept_connections, daemon=True).start()

    def _accept_connections(self):
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                ).start()
            except Exception:
                pass

    def _handle_client(self, client_socket: socket.socket):
        buffer = ""
        try:
            while self.running:
                data = client_socket.recv(65536).decode("utf-8")
                if not data:
                    break
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    msg_type = message.get("type")
                    if msg_type == "subscribe":
                        self._handle_subscription(message, client_socket)
                    elif msg_type == "register":
                        self._handle_register(message, client_socket)
                    elif msg_type == "subscribe_routed":
                        self._handle_routed_subscription(message)
                    elif msg_type == "subscribe_replicated":
                        self._handle_replicated_subscription(message)
                    elif msg_type == "match_forward":
                        self._handle_match_forward(message)
                    elif msg_type == "publication":
                        self._handle_publication(message)
        except Exception as e:
            self.logger.warning("socket_error error=%s", e)
        finally:
            client_socket.close()

    @staticmethod
    def _subscription_from_raw(raw_sub: dict) -> dict:
        return {
            field: tuple(cond) if isinstance(cond, list) else cond
            for field, cond in raw_sub.items()
        }

    def _get_target_broker(self, raw_sub: dict) -> str:
        sub_str = json.dumps(raw_sub, sort_keys=True)
        h = hashlib.md5(sub_str.encode("utf-8")).hexdigest()
        return self.hash_ring.get_node(h)

    def _store_active_subscription(
        self, subscriber_id: str, subscription: dict, entry_broker: str
    ):
        if subscriber_id not in self.subscriptions:
            self.subscriptions[subscriber_id] = []
        self.subscriptions[subscriber_id].append(
            {
                "subscription": subscription,
                "entry_broker": entry_broker,
            }
        )

    def _store_replicated_subscription(
        self,
        subscriber_id: str,
        subscription: dict,
        owner_broker: str,
        entry_broker: str,
    ):
        if subscriber_id not in self.replicated_subs:
            self.replicated_subs[subscriber_id] = []
        self.replicated_subs[subscriber_id].append(
            {
                "subscription": subscription,
                "owner_broker": owner_broker,
                "entry_broker": entry_broker,
            }
        )

    def _handle_subscription(self, message: Dict, client_socket: socket.socket):
        subscriber_id = message["subscriber_id"]
        raw_sub = message["subscription"]

        with self.lock:
            self.subscriber_sockets[subscriber_id] = client_socket

        target_broker = self._get_target_broker(raw_sub)
        self.logger.info(
            "sub_received subscriber_id=%s target_broker=%s",
            subscriber_id,
            target_broker,
        )

        if target_broker == self.broker_id:
            subscription = self._subscription_from_raw(raw_sub)
            with self.lock:
                self._store_active_subscription(
                    subscriber_id, subscription, self.broker_id
                )
            self.logger.info("sub_stored_local subscriber_id=%s", subscriber_id)
            if self.next_broker:
                rep_message = {
                    "type": "subscribe_replicated",
                    "subscriber_id": subscriber_id,
                    "subscription": raw_sub,
                    "owner_broker": self.broker_id,
                    "entry_broker": self.broker_id,
                }
                self._forward_message(rep_message, self.next_broker)
        else:
            target_addr = self.all_brokers.get(target_broker)
            if target_addr:
                routed_message = {
                    "type": "subscribe_routed",
                    "subscriber_id": subscriber_id,
                    "subscription": raw_sub,
                    "entry_broker": self.broker_id,
                }
                self._forward_message(routed_message, target_addr)
                self.logger.info(
                    "sub_routed subscriber_id=%s target_broker=%s",
                    subscriber_id,
                    target_broker,
                )

    def _handle_register(self, message: Dict, client_socket: socket.socket):
        subscriber_id = message["subscriber_id"]
        with self.lock:
            self.subscriber_sockets[subscriber_id] = client_socket
        self.logger.info("subscriber_registered subscriber_id=%s", subscriber_id)

    def _handle_routed_subscription(self, message: Dict):
        subscriber_id = message["subscriber_id"]
        raw_sub = message["subscription"]
        entry_broker = message["entry_broker"]

        subscription = self._subscription_from_raw(raw_sub)

        with self.lock:
            self._store_active_subscription(subscriber_id, subscription, entry_broker)

        self.logger.info(
            "sub_stored_routed subscriber_id=%s entry_broker=%s",
            subscriber_id,
            entry_broker,
        )

        if self.next_broker:
            rep_message = {
                "type": "subscribe_replicated",
                "subscriber_id": subscriber_id,
                "subscription": raw_sub,
                "owner_broker": self.broker_id,
                "entry_broker": entry_broker,
            }
            self._forward_message(rep_message, self.next_broker)

    def _handle_replicated_subscription(self, message: Dict):
        subscriber_id = message["subscriber_id"]
        raw_sub = message["subscription"]
        owner_broker = message["owner_broker"]
        entry_broker = message["entry_broker"]

        subscription = self._subscription_from_raw(raw_sub)

        with self.lock:
            self._store_replicated_subscription(
                subscriber_id,
                subscription,
                owner_broker,
                entry_broker,
            )

        self.logger.info(
            "sub_replicated subscriber_id=%s owner_broker=%s entry_broker=%s",
            subscriber_id,
            owner_broker,
            entry_broker,
        )

    def _handle_match_forward(self, message: Dict):
        subscriber_id = message["subscriber_id"]
        pub_data = message["publication"]
        self.logger.info("match_forward_received subscriber_id=%s", subscriber_id)
        self._notify_subscriber(subscriber_id, pub_data)

    def _handle_publication(self, message: Dict):
        pub_data = message["publication"]
        self.processed_pub_count += 1

        if not self._logged_sample:
            self._logged_sample = True
            self.logger.info("pub_encrypted_sample publication=%s", pub_data)
        elif self.processed_pub_count % 10000 == 0:
            self.logger.info(
                "pub_progress count=%d company=%s _ts=%s",
                self.processed_pub_count,
                pub_data.get("company"),
                pub_data.get("_ts"),
            )

        matches_to_forward = []
        local_matches = 0

        with self.lock:
            for sub_id, subs in self.subscriptions.items():
                for sub_entry in subs:
                    sub = sub_entry["subscription"]
                    entry_broker = sub_entry["entry_broker"]
                    if self.matching_engine.matches(pub_data, sub):
                        if entry_broker == self.broker_id:
                            self._notify_subscriber(sub_id, pub_data)
                            local_matches += 1
                        else:
                            matches_to_forward.append((entry_broker, sub_id, pub_data))
                        break

            for sub_id, rep_subs in self.replicated_subs.items():
                for rep in rep_subs:
                    owner = rep["owner_broker"]
                    if not self.broker_status.get(owner, True):
                        if self.matching_engine.matches(pub_data, rep["subscription"]):
                            matches_to_forward.append(
                                (rep["entry_broker"], sub_id, pub_data)
                            )
                            self.logger.info(
                                "failover_match subscriber_id=%s owner_broker=%s",
                                sub_id,
                                owner,
                            )
                            break

        if matches_to_forward or local_matches:
            self.logger.info(
                "pub_matched local_matches=%d forwards=%d active_subs=%d",
                local_matches,
                len(matches_to_forward),
                len(self.subscriptions),
            )

        for entry_broker_id, sub_id, fwd_pub in matches_to_forward:
            msg = {
                "type": "match_forward",
                "subscriber_id": sub_id,
                "publication": fwd_pub,
            }
            if self._forward_to_broker(entry_broker_id, msg):
                self.logger.info(
                    "match_forwarded subscriber_id=%s entry_broker=%s",
                    sub_id,
                    entry_broker_id,
                )

        if self.next_broker:
            self._forward_publication(message)

    def _notify_subscriber(self, sub_id: str, pub_data: Dict):
        if sub_id in self.subscriber_sockets:
            try:
                notification = (
                    json.dumps({"type": "match", "publication": pub_data}) + "\n"
                )
                self.subscriber_sockets[sub_id].sendall(notification.encode("utf-8"))
                self.logger.info("notify_sent subscriber_id=%s", sub_id)
            except Exception:
                self.logger.info("notify_failed subscriber_id=%s", sub_id)

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
                sock.sendall((json.dumps(message) + "\n").encode())
                return
        except Exception:
            with self.next_broker_lock:
                self.next_broker_socket = None
            for bid, addr in self.all_brokers.items():
                if addr == self.next_broker:
                    if self.broker_status.get(bid, True):
                        self.logger.warning("next_broker_unreachable broker_id=%s", bid)
                        self._mark_broker_down(self.next_broker)
                    break

    def _get_entry_broker_socket(self, broker_id: str):
        with self.entry_broker_lock:
            sock = self.entry_broker_sockets.get(broker_id)
            if sock is not None:
                return sock
            addr = self.all_brokers.get(broker_id)
            if not addr:
                return None
            try:
                s = socket.socket()
                s.settimeout(2)
                s.connect(addr)
                self.entry_broker_sockets[broker_id] = s
                return s
            except Exception:
                return None

    def _forward_to_broker(self, broker_id: str, message: Dict) -> bool:
        try:
            sock = self._get_entry_broker_socket(broker_id)
            if sock:
                sock.sendall((json.dumps(message) + "\n").encode())
                return True
        except Exception:
            with self.entry_broker_lock:
                self.entry_broker_sockets.pop(broker_id, None)
        return False

    def _forward_message(self, message: Dict, target: tuple):
        try:
            s = socket.socket()
            s.settimeout(2)
            s.connect(target)
            s.sendall((json.dumps(message) + "\n").encode())
            s.close()
        except Exception:
            pass

    def _mark_broker_down(self, address: tuple):
        for bid, addr in self.all_brokers.items():
            if addr == address:
                with self.lock:
                    self.broker_status[bid] = False
                self.logger.warning("broker_offline broker_id=%s", bid)
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
                            self.logger.info("broker_online broker_id=%s", bid)
                        self.broker_status[bid] = True
                except Exception:
                    with self.lock:
                        if self.broker_status.get(bid, True):
                            self.logger.warning(
                                "broker_offline_healthcheck broker_id=%s", bid
                            )
                        self.broker_status[bid] = False

    def start_kafka_ingestion(
        self, topic_name="raw-publications", bootstrap_servers="localhost:9092"
    ):
        def consume():
            config = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"{self.broker_id}-ingest",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
            consumer = None
            while self.running:
                try:
                    if consumer is None:
                        consumer = Consumer(config)
                        consumer.subscribe([topic_name])
                        self.logger.info("kafka_connected topic=%s", topic_name)
                    msg = consumer.poll(0.01)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        self.logger.error("kafka_error error=%s", msg.error())
                        continue

                    raw_str = msg.value().decode("utf-8", errors="ignore")
                    proto = publication_pb2.Publication()
                    proto.ParseFromString(base64.b64decode(raw_str))
                    pub_dict = {
                        "company": proto.company,
                        "value": proto.value,
                        "drop": proto.drop,
                        "variation": proto.variation,
                        "_ts": proto._ts,
                    }
                    self._handle_publication(
                        {"type": "publication", "publication": pub_dict}
                    )
                except Exception as e:
                    self.logger.warning("kafka_waiting error=%s", e)
                    if consumer is not None:
                        try:
                            consumer.close()
                        except Exception:
                            pass
                        consumer = None
                    time.sleep(2)

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
    parser.add_argument(
        "--log", default=None, help="Path to structured log file (optional)"
    )
    parser.add_argument(
        "--no-log", action="store_true", help="Disable all logging output"
    )
    args = parser.parse_args()

    bid = args.id
    host, port = BROKER_ADDRESSES[bid]
    nb = NEXT_BROKER[bid]
    next_host = nb[0] if nb else None
    next_port = nb[1] if nb else None

    logger, listener = setup_logger(bid, args.log, disable=args.no_log)

    broker = BrokerNode(
        bid,
        host,
        port,
        next_broker_host=next_host,
        next_broker_port=next_port,
        all_brokers=BROKER_ADDRESSES,
        logger=logger,
    )
    broker.start()

    if bid == "broker_0":
        broker.start_kafka_ingestion(topic_name=KAFKA_TOPIC)
        logger.info("kafka_ingest_started topic=%s", KAFKA_TOPIC)

    def shutdown(signum=None, frame=None):
        logger.info("shutting_down")
        broker.running = False
        stop_logger(listener)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
