import socket, json, threading, time, statistics

BROKER_ADDRESSES = {
    "broker_0": ("localhost", 8001),
    "broker_1": ("localhost", 8002),
    "broker_2": ("localhost", 8003),
}

match_count = 0
latencies = []
lock = threading.Lock()

def listen(broker_id, host, port):
    s = socket.socket()
    s.connect((host, port))
    msg = {
        "type": "subscribe",
        "subscriber_id": "eval_client",
        "subscription": {"value": (">", 0)}
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
                    sent_ms = msg["publication"].get("_ts")
                    with lock:
                        global match_count
                        match_count += 1
                        if sent_ms:
                            latencies.append(recv_ms - sent_ms)
            except Exception:
                pass

for bid, (host, port) in BROKER_ADDRESSES.items():
    threading.Thread(target=listen, args=(bid, host, port), daemon=True).start()

print("Ascult match-uri... Porneste Java acum. Ctrl+C dupa ce Java termina.")
try:
    while True:
        time.sleep(10)
        print(f"  Match-uri pana acum: {match_count}")
except KeyboardInterrupt:
    pass

print(f"\n{'='*45}")
print(f"  Total match-uri livrate:  {match_count}")
if latencies:
    s = sorted(latencies)
    print(f"  Latenta medie:            {statistics.mean(latencies):.2f} ms")
    print(f"  Latenta mediana (p50):    {statistics.median(latencies):.2f} ms")
    print(f"  Latenta p95:              {s[int(0.95*len(s))]:.2f} ms")
    print(f"  Latenta p99:              {s[int(0.99*len(s))]:.2f} ms")
else:
    print("  Latenta: N/A (lipseste _ts din publicatii)")
print(f"{'='*45}")