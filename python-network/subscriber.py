import socket
import json
import threading
import time
import argparse
import queue
from typing import Dict, Tuple
from consistent_hash import ConsistentHashRing
from utils import parse_java_subscription, encrypt_subscription, decrypt_publication
from matching_engine import MatchingEngine


class Subscriber:
    def __init__(self, subscriber_id: str, broker_addresses: Dict[str, Tuple[str, int]],
                 verify_matches: bool = False):
        self.subscriber_id = subscriber_id
        self.broker_addresses = broker_addresses

        self.hash_ring = ConsistentHashRing(list(broker_addresses.keys()))
        self.sockets = {}

        self._sub_counter = 0

        self.broker_sub_count = {bid: 0 for bid in broker_addresses}

        self.verify_matches = verify_matches
        self.raw_subs = []
        self.matching_engine = MatchingEngine()
        self._verified = 0
        self._failed = 0
        self._verify_lock = threading.Lock()

        self.match_log_file = open(f"{subscriber_id}_matches.csv", "w")
        self.match_log_file.write("elapsed_ms,broker_id\n")
        self.log_queue = queue.Queue()
        self._log_writer_stop = threading.Event()
        self._log_first_ts = None
        self._log_ts_lock = threading.Lock()
        self._log_writer_thread = threading.Thread(target=self._log_writer, daemon=True)
        self._log_writer_thread.start()

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

                    if self.verify_matches:
                        self.raw_subs.append(sub_dict)

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
                        self.broker_sub_count[target_broker] += 1

            if self.verify_matches and self.raw_subs:
                print(f"[{self.subscriber_id}] Stored {len(self.raw_subs)} raw subs for verification")

            self.print_balance_stats()
            print(f"[Subscriber {self.subscriber_id}] Subscriptions routed successfully.")
        except FileNotFoundError:
            print("Subscriptions file not found.")

    def print_balance_stats(self):
        total = sum(self.broker_sub_count.values())
        if total == 0:
            return
        counts = list(self.broker_sub_count.values())
        mean = sum(counts) / len(counts)
        variance = sum((c - mean) ** 2 for c in counts) / len(counts)
        print(f"[{self.subscriber_id}] === Subscription Balance ===")
        for bid, count in sorted(self.broker_sub_count.items()):
            pct = count / total * 100
            print(f"  {bid}: {count} ({pct:.1f}%)")
        print(f"  Total: {total}, Mean: {mean:.1f}, StdDev: {variance ** 0.5:.1f}")

    def _log_writer(self):
        while not self._log_writer_stop.is_set():
            try:
                ts, broker_id = self.log_queue.get(timeout=0.5)
                with self._log_ts_lock:
                    if self._log_first_ts is None:
                        self._log_first_ts = ts
                    elapsed = ts - self._log_first_ts
                lines = [f"{elapsed},{broker_id}\n"]
                while not self.log_queue.empty():
                    ts, broker_id = self.log_queue.get_nowait()
                    with self._log_ts_lock:
                        elapsed = ts - self._log_first_ts
                    lines.append(f"{elapsed},{broker_id}\n")
                self.match_log_file.write("".join(lines))
            except queue.Empty:
                pass

    def _log_match(self, ts, broker_id):
        if ts is not None:
            self.log_queue.put((ts, broker_id))

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

                        self._log_match(ts, broker_id)

                        if self.verify_matches:
                            self._verify_match(pub, broker_id)
                            continue

                        print(f"[{self.subscriber_id}] MATCH received from {broker_id}: {pub}")
        except Exception:
            pass

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

        status = "✓" if verified else "✗"
        print(f"[{self.subscriber_id}] {status} MATCH from {broker_id}: decrypted={plain_pub}")
        if verified:
            print(f"  └─ matched sub: {matched_sub}")
        else:
            print(f"  └─ WARNING: no matching subscription found!")
            for sub in self.raw_subs[:3]:
                print(f"     sample raw sub: {sub}")


BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}


def main():
    parser = argparse.ArgumentParser(description="Start a subscriber node")
    parser.add_argument("--id", required=True, choices=["client_1", "client_2", "client_3"])
    parser.add_argument("--subscriptions", default="subscriptions.txt")
    parser.add_argument("--verify-matches", action="store_true",
                        help="decrypt match notifications and assert they match a subscription")
    args = parser.parse_args()

    sub = Subscriber(args.id, BROKER_ADDRESSES, verify_matches=args.verify_matches)
    sub.connect_to_brokers()
    time.sleep(2)
    sub.load_and_route_subscriptions(args.subscriptions)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sub.print_balance_stats()
        sub._log_writer_stop.set()
        sub._log_writer_thread.join(timeout=2)
        sub.match_log_file.flush()
        sub.match_log_file.close()
        if args.verify_matches:
            total = sub._verified + sub._failed
            print(f"[{args.id}] === Match verification ===")
            print(f"  Verified: {sub._verified}/{total} ({(sub._verified / total * 100) if total else 0:.1f}%)")
            print(f"  Failed:   {sub._failed}/{total} ({(sub._failed / total * 100) if total else 0:.1f}%)")
        print(f"\n[{args.id}] Shutting down.")


if __name__ == "__main__":
    main()