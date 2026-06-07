import time
import base64
import argparse
from kafka import KafkaProducer
import publication_pb2
from utils import parse_java_publication, _hash_text, _ope_encrypt


KAFKA_TOPIC = "raw-publications"
BROKER = "localhost:9092"
SEND_INTERVAL = 0.002


def stream_publications(filepath: str, duration_minutes: float = 3):
    with open(filepath) as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        print("No publications to publish.")
        return

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda m: m.encode(),
    )

    start = time.time()
    duration_ms = duration_minutes * 60 * 1000
    idx = 0
    count = 0

    print(f"Streaming {len(lines)} publications for {duration_minutes} min...")

    while (time.time() - start) * 1000 < duration_ms:
        raw = lines[idx % len(lines)]
        pub = parse_java_publication(raw)

        current_ts = int(time.time() * 1000)

        proto = publication_pb2.Publication(
            company=_hash_text(str(pub.get("company", ""))),
            value=_ope_encrypt(float(pub.get("value", 0))),
            drop=_ope_encrypt(float(pub.get("drop", 0))),
            variation=_ope_encrypt(float(pub.get("variation", 0))),
            date=_hash_text(str(pub.get("date", ""))),
            _ts=current_ts,
        )

        encoded = base64.b64encode(proto.SerializeToString()).decode()
        producer.send(KAFKA_TOPIC, encoded)
        count += 1
        idx += 1

        if count % 10000 == 0:
            elapsed = time.time() - start
            print(f"  Sent: {count} publications in {elapsed:.1f}s")

        time.sleep(SEND_INTERVAL)

    elapsed = time.time() - start
    print(f"Done. Sent {count} publications in {elapsed:.1f}s.")
    producer.flush()
    producer.close()


def main():
    parser = argparse.ArgumentParser(description="Start the publisher node")
    parser.add_argument("--publications", default="publications.txt")
    parser.add_argument("--duration", type=float, default=3)
    args = parser.parse_args()

    stream_publications(args.publications, args.duration)


if __name__ == "__main__":
    main()
