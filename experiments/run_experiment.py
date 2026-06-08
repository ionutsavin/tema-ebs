#!/usr/bin/env python3
"""Shared experiment runner: Kafka, brokers, subscribers, publisher, aggregate."""

import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JAVA_DIR = os.path.join(ROOT, "java-publication")
PYTHON_DIR = os.path.join(ROOT, "python-network")
COMPOSE_FILE = os.path.join(ROOT, "docker-compose.yml")

BROKERS = ["broker_0", "broker_1", "broker_2"]
SUBSCRIBERS = ["client_1", "client_2", "client_3"]
PUBLISHER_DURATION_MIN = 3
DRAIN_SECONDS = 20
SILENT = subprocess.DEVNULL


def _popen(args, *, cwd=None, silent=False):
    kwargs = {"cwd": cwd}
    if silent:
        kwargs["stdout"] = SILENT
        kwargs["stderr"] = SILENT
    return subprocess.Popen(args, **kwargs)


def _run(args, *, cwd=None, check=True):
    return subprocess.run(args, cwd=cwd, check=check)


def _stop_processes(processes, sig=signal.SIGTERM, timeout=5):
    for p in processes:
        if p.poll() is None:
            p.send_signal(sig)
    for p in processes:
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def run_experiment(scenario_dir, *, start_kafka=True, stop_kafka=True):
    scenario = os.path.basename(scenario_dir)
    brokers = []
    subscribers = []

    print(f"\n=== Experiment: {scenario} ===", flush=True)

    try:
        if start_kafka:
            print("--- 1. Starting Kafka ---", flush=True)
            _run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d"])
            time.sleep(5)

        print("--- 2. Generating data (Java) ---", flush=True)
        result = subprocess.run(
            ["./gradlew", "run", "--args=-c "
             f"{scenario_dir}/input.json -o {scenario_dir}"],
            cwd=JAVA_DIR, check=True, capture_output=True, text=True,
        )
        for line in result.stdout.strip().split("\n")[-5:]:
            if line.strip():
                print(line)

        print("--- 3. Starting brokers ---", flush=True)
        for bid in BROKERS:
            brokers.append(_popen(
                ["uv", "run", "broker.py", "--id", bid, "--no-log"],
                cwd=PYTHON_DIR, silent=True,
            ))
            time.sleep(1)
        time.sleep(3)

        print("--- 4. Starting subscribers ---", flush=True)
        for sid in SUBSCRIBERS:
            subscribers.append(_popen(
                ["uv", "run", "--project", PYTHON_DIR,
                 os.path.join(PYTHON_DIR, "subscriber.py"),
                 "--id", sid,
                 "--subscriptions", os.path.join(scenario_dir, "subscriptions.txt"),
                 "--log", os.path.join(scenario_dir, f"{sid}.log"),
                 "--verify-matches"],
                cwd=scenario_dir, silent=True,
            ))
            time.sleep(1)
        time.sleep(3)

        print(f"--- 5. Running publisher ({PUBLISHER_DURATION_MIN} minutes) ---", flush=True)
        metrics_path = os.path.join(scenario_dir, "publisher_metrics.json")
        _run(
            ["uv", "run", "--project", PYTHON_DIR,
             os.path.join(PYTHON_DIR, "publisher.py"),
             "--publications", os.path.join(scenario_dir, "publications.txt"),
             "--duration", str(PUBLISHER_DURATION_MIN),
             "--no-log",
             "--metrics-out", metrics_path],
            cwd=scenario_dir,
        )

        print(f"--- 6. Draining pipeline ({DRAIN_SECONDS}s) ---", flush=True)
        time.sleep(DRAIN_SECONDS)

        print("--- 7. Stopping subscribers ---", flush=True)
        _stop_processes(subscribers)

        print("--- 8. Aggregating results ---", flush=True)
        _run([sys.executable, os.path.join(scenario_dir, "aggregate.py")], cwd=scenario_dir)

    finally:
        _stop_processes(brokers, sig=signal.SIGTERM, timeout=3)
        if stop_kafka and start_kafka:
            subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "down"],
                capture_output=True,
            )
