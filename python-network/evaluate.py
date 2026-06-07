import socket
import json
import threading
import time
import statistics

BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}

match_count = 0
latencies = []
lock = threading.Lock()


def listen(broker_id, host, port):
    try:
        s = socket.socket()
        s.connect((host, port))

        # OPE_SHIFT for value 0 is 8921.45.
        # So "value > 0" becomes "encrypted_value > 8921.45"
        msg = {
            "type": "subscribe",
            "subscriber_id": f"eval_client_{broker_id}",
            "subscription": {"value": [">", 8921.45]}
        }
        s.send((json.dumps(msg) + "\n").encode())

        buf = ""
        while True:
            data = s.recv(65536).decode("utf-8", errors="ignore")
            if not data:
                break
            buf += data
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                    if msg.get("type") == "match":
                        recv_ms = time.time() * 1000
                        pub = msg.get("publication", {})

                        sent_ms = pub.get("_ts")

                        with lock:
                            global match_count
                            match_count += 1
                            if sent_ms is not None:
                                try:
                                    latencies.append(recv_ms - float(sent_ms))
                                except ValueError:
                                    pass
                except Exception:
                    pass
    except Exception as e:
        print(f"Connection error to {broker_id}: {e}")


for bid, (host, port) in BROKER_ADDRESSES.items():
    threading.Thread(target=listen, args=(bid, host, port), daemon=True).start()

print("Listening for matches... Start Java now. Press Ctrl+C when Java finishes.")
try:
    while True:
        time.sleep(5)
        print(f"  Matches collected so far: {match_count}")
except KeyboardInterrupt:
    pass

print(f"\n{'=' * 45}")
print(f"  Total matches delivered:  {match_count}")
if latencies:
    s = sorted(latencies)
    print(f"  Average latency:          {statistics.mean(latencies):.2f} ms")
    print(f"  Median latency (p50):     {statistics.median(latencies):.2f} ms")
    print(f"  Latency p95:              {s[int(0.95 * len(s))]:.2f} ms")
    print(f"  Latency p99:              {s[int(0.99 * len(s))]:.2f} ms")
else:
    print("  Latency: N/A (missing _ts in publications to compute)")
print(f"{'=' * 45}")