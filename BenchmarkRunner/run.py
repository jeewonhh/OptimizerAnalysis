import duckdb

import os
import motherduck
from test_case import *

TPCH = TestCase(
    benchmark="tpch",
    explain=False,
    raise_on_error=True,
)

TPCDS = TestCase(
    benchmark="tpcds",
    explain=False,
    raise_on_error=True,
)

JOB = TestCase(
    benchmark="job",
    explain=False,
    raise_on_error=True,
)

OG = Optimizer(
    type=OptimizerType.HEURISTIC
)

CD = Optimizer(
    type=OptimizerType.DP,
    estimation_function=EstimationFunction.CARDINALITY
)

TS = Optimizer(
    type=OptimizerType.DP,
    estimation_function=EstimationFunction.TABLE_SIZE
)

DS = Optimizer(
    type=OptimizerType.DP,
    estimation_function=EstimationFunction.DATA_SIZE
)

DS_SIMPLIFIED = Optimizer(
    type=OptimizerType.DP,
    estimation_function=EstimationFunction.DATA_SIZE_SIMPLIFIED
)

BENCHMARKS = [TPCH, TPCDS, JOB]
OPTIMIZERS = [OG, CD, TS, DS, DS_SIMPLIFIED]

results = TPCH.run(OG)
print(results)


def run_explain_queries(benchmark: str, index_file: str):
    pass

def run_queries(benchmark: str, index_file: str):
    pass

