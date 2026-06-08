#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from run_experiment import run_experiment


if __name__ == "__main__":
    run_experiment(os.path.dirname(os.path.abspath(__file__)))
