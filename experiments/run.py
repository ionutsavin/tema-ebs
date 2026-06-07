#!/usr/bin/env python3
import subprocess
import sys
import time
import os
import signal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JAVA_DIR = os.path.join(ROOT, "java-publication")
PYTHON_DIR = os.path.join(ROOT, "python-network")


def run_experiment(scenario_dir):
    scenario = os.path.basename(scenario_dir)
    print(f"\n=== Experiment: {scenario} ===", flush=True)

    print("--- 1. Generating data (Java) ---", flush=True)
    result = subprocess.run(
        f'./gradlew run --args="-c {scenario_dir}/input.json -o {scenario_dir}"',
        cwd=JAVA_DIR, check=True, capture_output=True, text=True, shell=True
    )
    for line in result.stdout.strip().split("\n")[-5:]:
        print(line)
    if result.stderr.strip():
        for line in result.stderr.strip().split("\n")[-3:]:
            print(line)

    print("--- 2. Starting brokers ---", flush=True)
    brokers = []
    for bid in ["broker_0", "broker_1", "broker_2"]:
        p = subprocess.Popen(
            ["uv", "run", "broker.py", "--id", bid],
            cwd=PYTHON_DIR
        )
        brokers.append(p)
        time.sleep(1)
    time.sleep(3)

    print("--- 3. Starting subscribers ---", flush=True)
    subscribers = []
    for sid in ["client_1", "client_2", "client_3"]:
        p = subprocess.Popen(
            ["uv", "run", "--project", PYTHON_DIR,
             os.path.join(PYTHON_DIR, "subscriber.py"),
             "--id", sid,
             "--subscriptions", os.path.join(scenario_dir, "subscriptions.txt"),
             "--verify-matches"],
            cwd=scenario_dir
        )
        subscribers.append(p)
        time.sleep(1)
    time.sleep(3)

    print("--- 4. Running publisher (3 minutes) ---", flush=True)
    subprocess.run(
        ["uv", "run", "--project", PYTHON_DIR,
         os.path.join(PYTHON_DIR, "publisher.py"),
         "--publications", os.path.join(scenario_dir, "publications.txt"),
         "--duration", "3"],
        cwd=scenario_dir, check=True
    )

    print("--- 5. Aggregating results ---", flush=True)
    for p in subscribers:
        p.send_signal(signal.SIGTERM)
    for p in subscribers:
        p.wait(timeout=5)
    for p in brokers:
        p.terminate()
    for p in brokers:
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()

    subprocess.run([sys.executable, os.path.join(scenario_dir, "aggregate.py")], cwd=scenario_dir)


def main():
    experiments_dir = os.path.dirname(os.path.abspath(__file__))

    for scenario in ["100pct_eq", "25pct_eq"]:
        scenario_dir = os.path.join(experiments_dir, scenario)
        run_experiment(scenario_dir)

    print("\n==========================================")
    print("  All experiments completed.")
    print("==========================================", flush=True)


if __name__ == "__main__":
    main()
