from run import *

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Make end to end sql script with given benchmark and optimizer')
    parser.add_argument('-b', '--benchmark', type=str, help='Benchmark to be used')
    parser.add_argument('-o', '--optimizer', type=str, help='Optimizer to be used')
    parser.add_argument('-e', '--explain', type=bool, help='Make explain script')

    # Parse the arguments
    args = parser.parse_args()

    run_test_case(args.optimizer, args.benchmark, args.explain)

