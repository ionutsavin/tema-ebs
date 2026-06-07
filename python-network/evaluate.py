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

        # OPE_SHIFT pentru valoarea 0 este 8921.45.
        # Astfel, "value > 0" devine "value_criptat > 8921.45"
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
        print(f"Eroare conectare la {broker_id}: {e}")


for bid, (host, port) in BROKER_ADDRESSES.items():
    threading.Thread(target=listen, args=(bid, host, port), daemon=True).start()

print("Ascult match-uri... Porneste Java acum. Apasa Ctrl+C dupa ce Java termina.")
try:
    while True:
        time.sleep(5)
        print(f"  Match-uri colectate pana acum: {match_count}")
except KeyboardInterrupt:
    pass

print(f"\n{'=' * 45}")
print(f"  Total match-uri livrate:  {match_count}")
if latencies:
    s = sorted(latencies)
    print(f"  Latenta medie:            {statistics.mean(latencies):.2f} ms")
    print(f"  Latenta mediana (p50):    {statistics.median(latencies):.2f} ms")
    print(f"  Latenta p95:              {s[int(0.95 * len(s))]:.2f} ms")
    print(f"  Latenta p99:              {s[int(0.99 * len(s))]:.2f} ms")
else:
    print("  Latenta: N/A (lipseste _ts in clar din publicatii pentru a calcula)")
print(f"{'=' * 45}")