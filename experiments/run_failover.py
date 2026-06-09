#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_experiment_failover import run_experiment_failover


def main():
    experiments_dir = os.path.dirname(os.path.abspath(__file__))

    broker_to_kill = "broker_1"

    for scenario in ["100pct_eq", "25pct_eq"]:
        run_experiment_failover(
            os.path.join(experiments_dir, scenario), 
            broker_to_kill=broker_to_kill
        )

    print("\n==========================================")
    print("  All Failover experiments completed.")
    print("==========================================", flush=True)


if __name__ == "__main__":
    main()