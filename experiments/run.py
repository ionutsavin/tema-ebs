#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_experiment import run_experiment


def main():
    experiments_dir = os.path.dirname(os.path.abspath(__file__))

    for scenario in ["100pct_eq", "25pct_eq"]:
        run_experiment(os.path.join(experiments_dir, scenario))

    print("\n==========================================")
    print("  All experiments completed.")
    print("==========================================", flush=True)


if __name__ == "__main__":
    main()
