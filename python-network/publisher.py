import time
import base64
import argparse
import json
import logging
from confluent_kafka import Producer
import publication_pb2
from system_logger import setup_logger, stop_logger
from utils import parse_java_publication, _encrypt_text, _ope_encrypt


KAFKA_TOPIC = "raw-publications"
BROKER = "localhost:9092"
SEND_INTERVAL = 0.002


def stream_publications(
    filepath: str,
    duration_minutes: float,
    logger: logging.Logger,
    metrics_out: str | None = None,
):
    with open(filepath) as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        logger.warning("no_publications path=%s", filepath)
        return

    producer = Producer({"bootstrap.servers": BROKER})

    start = time.time()
    duration_ms = duration_minutes * 60 * 1000
    idx = 0
    count = 0

    logger.info(
        "stream_started publication_count=%d duration_min=%.2f",
        len(lines), duration_minutes,
    )

    while (time.time() - start) * 1000 < duration_ms:
        raw = lines[idx % len(lines)]
        pub = parse_java_publication(raw)

        current_ts = int(time.time() * 1000)

        proto = publication_pb2.Publication(
            company=_encrypt_text(str(pub.get("company", ""))),
            value=_ope_encrypt(float(pub.get("value", 0))),
            drop=_ope_encrypt(float(pub.get("drop", 0))),
            variation=_ope_encrypt(float(pub.get("variation", 0))),
            _ts=current_ts,
        )

        encoded = base64.b64encode(proto.SerializeToString()).decode()
        producer.produce(KAFKA_TOPIC, encoded.encode("utf-8"))
        producer.poll(0)
        count += 1
        idx += 1

        if count % 1000 == 0:
            logger.info("pub_sent count=%d company=%s", count, pub.get("company"))

        if count % 10000 == 0:
            elapsed = time.time() - start
            logger.info("pub_progress count=%d elapsed_s=%.1f", count, elapsed)

        time.sleep(SEND_INTERVAL)

    elapsed = time.time() - start
    logger.info("pub_done total_sent=%d elapsed_s=%.1f", count, elapsed)
    producer.flush()

    if metrics_out:
        with open(metrics_out, "w", encoding="utf-8") as f:
            json.dump({
                "pubs_sent": count,
                "duration_s": round(elapsed, 3),
                "feed_duration_min": duration_minutes,
            }, f)


def main():
    parser = argparse.ArgumentParser(description="Start the publisher node")
    parser.add_argument("--publications", default="publications.txt")
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--log", default=None, help="Path to structured log file (optional)")
    parser.add_argument("--no-log", action="store_true", help="Disable all logging output")
    parser.add_argument("--metrics-out", default=None, help="Write publish metrics JSON (optional)")
    args = parser.parse_args()

    logger, listener = setup_logger("publisher", args.log, disable=args.no_log)

    stream_publications(
        args.publications, args.duration, logger=logger, metrics_out=args.metrics_out,
    )

    stop_logger(listener)


if __name__ == "__main__":
    main()
