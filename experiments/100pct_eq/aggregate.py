import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from aggregate_utils import collect_results, print_results


def main():
    scenario_dir = os.path.dirname(os.path.abspath(__file__))
    print_results(collect_results(scenario_dir))


if __name__ == "__main__":
    main()
