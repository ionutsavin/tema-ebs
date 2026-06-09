import socket
import json
import threading
import time
import argparse
import signal
import sys
import random
import logging
from typing import Dict, Tuple
from utils import parse_java_subscription, encrypt_subscription, decrypt_publication
from matching_engine import MatchingEngine
from system_logger import setup_logger, stop_logger


class Subscriber:
    def __init__(
        self,
        subscriber_id: str,
        broker_addresses: Dict[str, Tuple[str, int]],
        logger: logging.Logger,
        verify_matches: bool = False,
    ):
        self.subscriber_id = subscriber_id
        self.broker_addresses = broker_addresses
        self.logger = logger

        self.brokers_list = list(broker_addresses.keys())
        self.broker_idx = random.randint(0, len(self.brokers_list) - 1)
        self.socket = None
        self.is_failover = False
        self.last_ts = 0

        self.verify_matches = verify_matches
        self.raw_subs = []
        self.encrypted_subs = []
        self.matching_engine = MatchingEngine()
        self._verified = 0
        self._failed = 0
        self._verify_lock = threading.Lock()

    def connect_to_broker(self):
        attempts = 0
        while True:
            broker_id = self.brokers_list[self.broker_idx]
            host, port = self.broker_addresses[broker_id]
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((host, port))
                
                register_msg = {
                    "type": "register",
                    "subscriber_id": self.subscriber_id,
                    "is_failover": self.is_failover,
                    "last_ts": self.last_ts
                }
                self.socket.sendall((json.dumps(register_msg) + "\n").encode("utf-8"))

                if self.is_failover:
                    for sub in self.encrypted_subs:
                        subscribe_msg = {
                            "type": "subscribe",
                            "subscriber_id": self.subscriber_id,
                            "subscription": sub,
                            "is_failover": self.is_failover,
                            "last_ts": self.last_ts
                        }
                        self.socket.sendall((json.dumps(subscribe_msg) + "\n").encode("utf-8"))

                self.logger.info("Connected to %s (failover=%s), last_ts=%s", broker_id, self.is_failover, self.last_ts)
                
                threading.Thread(
                    target=self._listen_for_matches, args=(broker_id, self.socket), daemon=True
                ).start()
                break

            except Exception as e:
                self.logger.warning("Connection failed to %s: %s", broker_id, e)
                time.sleep(1)
                attempts += 1
                self.broker_idx = (self.broker_idx + 1) % len(self.brokers_list)
                self.is_failover = True

    def load_and_send_subscriptions(self, filepath: str):
        sent_count = 0
        try:
            with open(filepath, "r") as f:
                for line in f:
                    sub_str = line.strip()
                    if not sub_str:
                        continue

                    sub_dict = parse_java_subscription(sub_str)
                    if not sub_dict:
                        continue

                    if self.verify_matches:
                        self.raw_subs.append(sub_dict)

                    encrypted_sub = encrypt_subscription(sub_dict)
                    if not encrypted_sub:
                        continue

                    self.encrypted_subs.append(encrypted_sub)

                    message = {
                        "type": "subscribe",
                        "subscriber_id": self.subscriber_id,
                        "subscription": encrypted_sub,
                        "is_failover": False,
                        "last_ts": 0
                    }
                    if self.socket:
                        self.socket.sendall(
                            (json.dumps(message) + "\n").encode("utf-8")
                        )
                        sent_count += 1

            if self.verify_matches and self.raw_subs:
                self.logger.info("verify_subs_stored count=%d", len(self.raw_subs))

            self.logger.info(
                "subs_sent count=%d broker_id=%s",
                sent_count, self.brokers_list[self.broker_idx],
            )
        except FileNotFoundError:
            self.logger.error("subscriptions_file_not_found path=%s", filepath)

    def _log_match(self, ts, broker_id):
        if ts is not None:
            latency = int(time.time() * 1000) - ts
            self.logger.info(
                "match_received ts=%s latency_ms=%d broker_id=%s",
                ts, latency, broker_id,
            )

    def _listen_for_matches(self, broker_id: str, sock: socket.socket):
        buffer = ""
        try:
            while True:
                data = sock.recv(65536).decode("utf-8")
                if not data:
                    raise ConnectionError("EOF or disconnect from broker.")
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("type") == "match":
                        pub = msg["publication"]
                        ts = pub.get("_ts")
                        if ts is not None:
                            self.last_ts = max(self.last_ts, int(ts))
                        self._log_match(ts, broker_id)
                        if self.verify_matches:
                            self._verify_match(pub, broker_id)
        except Exception as e:
            self.logger.warning("Lost connection to %s: %s", broker_id, e)
            try:
                sock.close()
            except Exception:
                pass
            self.is_failover = True
            self.broker_idx = (self.broker_idx + 1) % len(self.brokers_list)
            self.connect_to_broker()

    def _verify_match(self, encrypted_pub: dict, broker_id: str):
        plain_pub = decrypt_publication(encrypted_pub)

        verified = False
        matched_sub = None
        for sub in self.raw_subs:
            if self.matching_engine.matches(plain_pub, sub):
                verified = True
                matched_sub = sub
                break

        with self._verify_lock:
            if verified:
                self._verified += 1
            else:
                self._failed += 1

        if verified:
            self.logger.info(
                "verify_ok broker_id=%s matched_sub=%s",
                broker_id, matched_sub,
            )
        else:
            self.logger.warning(
                "verify_failed broker_id=%s decrypted=%s sample_subs=%s",
                broker_id, plain_pub, self.raw_subs[:3],
            )


BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}


def main():
    parser = argparse.ArgumentParser(description="Start a subscriber node")
    parser.add_argument("--id", required=True, choices=["client_1", "client_2", "client_3"])
    parser.add_argument("--subscriptions", default="subscriptions.txt")
    parser.add_argument("--log", default=None, help="Path to structured log file (optional)")
    parser.add_argument("--no-log", action="store_true", help="Disable all logging output")
    parser.add_argument(
        "--verify-matches", action="store_true",
        help="decrypt match notifications and assert they match a subscription",
    )
    args = parser.parse_args()

    logger, listener = setup_logger(args.id, args.log, disable=args.no_log)

    sub = Subscriber(args.id, BROKER_ADDRESSES, logger, verify_matches=args.verify_matches)
    sub.connect_to_broker()
    time.sleep(2)
    sub.load_and_send_subscriptions(args.subscriptions)

    def shutdown(signum=None, frame=None):
        logger.info("shutting_down")
        if args.verify_matches:
            total = sub._verified + sub._failed
            logger.info(
                "verify_summary verified=%d failed=%d total=%d rate=%.1f",
                sub._verified,
                sub._failed,
                total,
                (sub._verified / total * 100) if total else 0.0,
            )
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