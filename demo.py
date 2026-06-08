import subprocess
import time
import signal
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
JAVA_DIR = os.path.join(ROOT, "java-publication")
PYTHON_DIR = os.path.join(ROOT, "python-network")
DURATION = 3

children = []


def cleanup():
    print("\nShutting down nodes...")
    for p in children:
        try:
            p.terminate()
            p.wait(timeout=3)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
    subprocess.run(["docker", "compose", "-f", os.path.join(ROOT, "docker-compose.yml"), "down"],
                   capture_output=True)
    print("Done.")


def run(args, cwd=None, background=False):
    if background:
        p = subprocess.Popen(args, cwd=cwd)
        children.append(p)
        return p
    return subprocess.run(args, cwd=cwd)


def main():
    signal.signal(signal.SIGINT, lambda s, f: (cleanup(), sys.exit(0)))
    signal.signal(signal.SIGTERM, lambda s, f: (cleanup(), sys.exit(0)))

    print("=== 1. Starting Kafka ===")
    subprocess.run(["docker", "compose", "-f", os.path.join(ROOT, "docker-compose.yml"),
                    "up", "-d"], capture_output=True)
    time.sleep(5)

    print("=== 2. Generating data (Java) ===")
    run(["./gradlew", "run", "--args=-o " + PYTHON_DIR], cwd=JAVA_DIR)

    print("=== 3. Starting brokers ===")
    for bid in ["broker_0", "broker_1", "broker_2"]:
        run(["uv", "run", "broker.py", "--id", bid], cwd=PYTHON_DIR, background=True)
        time.sleep(0.5)

    time.sleep(2)

    print("=== 4. Starting subscribers ===")
    for sid in ["client_1", "client_2", "client_3"]:
        run(["uv", "run", "subscriber.py", "--id", sid,
             "--subscriptions", os.path.join(PYTHON_DIR, "subscriptions.txt")],
            cwd=PYTHON_DIR, background=True)
        time.sleep(0.5)

    time.sleep(2)

    print(f"=== 5. Starting publisher (for {DURATION} minutes) ===")
    run(["uv", "run", "publisher.py",
         "--publications", os.path.join(PYTHON_DIR, "publications.txt"),
         "--duration", str(DURATION)],
        cwd=PYTHON_DIR)

    cleanup()


if __name__ == "__main__":
    main()
