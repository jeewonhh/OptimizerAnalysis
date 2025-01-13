import duckdb

import os
import motherduck
from pathlib import Path

from test_case import *

BENCHMARKS = [
    TPCH := TestCase(
        benchmark="tpch",
        raise_on_error=True,
    ),
    TPCDS := TestCase(
        benchmark="tpcds",
        raise_on_error=True,
    ),
    JOB := TestCase(
        benchmark="job",
        raise_on_error=True,
    )
]

OPTIMIZERS = [
    OG := Optimizer(
        type=OptimizerType.HEURISTIC
    ),
    TS := Optimizer(
        type=OptimizerType.DP,
        estimation_function=EstimationFunction.TABLE_SIZE
    ),
    DS := Optimizer(
        type=OptimizerType.DP,
        estimation_function=EstimationFunction.DATA_SIZE
    ),
    DS_SIMPLIFIED := Optimizer(
        type=OptimizerType.DP,
        estimation_function=EstimationFunction.DATA_SIZE_SIMPLIFIED
    )
]

BRIDGE_COST = 10_000


def run_test_case(optimizer: Optimizer, test_case: TestCase, explain: bool = False):
    os.environ["OPTIMIZER"] = optimizer.type.name
    if optimizer.estimation_function:
        os.environ["ESTIMATION"] = optimizer.estimation_function.name
    os.environ["BRIDGE_COST"] = str(BRIDGE_COST)

    logger.info(f"Starting Test Case [{test_case.benchmark}] with Optimizer [{optimizer.to_string()}]")
    test_case.run(optimizer, explain=explain)


def has_complete_explain_queries(optimizer: Optimizer, test_case: TestCase):
    plans_folder = os.path.join(RESULT_ROOT, test_case.benchmark, optimizer.to_string(), "plans")
    if not os.path.exists(plans_folder):
        return False

    query_ids = {Path(file_name).stem for file_name in os.listdir(test_case.path)}

    return query_ids == set(os.listdir(str(plans_folder)))


def identify_differentiating_queries(optimizer: Optimizer, baseline_optimizer: Optimizer, test_case: TestCase):
    if not has_complete_explain_queries(optimizer, test_case):
        run_test_case(optimizer, test_case, explain=True)

    if not has_complete_explain_queries(baseline_optimizer, test_case):
        run_test_case(baseline_optimizer, test_case, explain=True)

    differentiating_queries = {}

    query_ids = [Path(file_name).stem for file_name in os.listdir(test_case.path)]

    plans_folder = os.path.join(RESULT_ROOT, test_case.benchmark, optimizer.to_string(), "plans")
    baseline_plans_folder = os.path.join(RESULT_ROOT, test_case.benchmark, baseline_optimizer.to_string(), "plans")

    for query_id in query_ids:
        differentiating_queries[query_id] = []
        for variation_file in os.listdir(str(plans_folder) + "/" + query_id):
            plan = open(os.path.join(str(plans_folder), query_id, variation_file)).read()
            baseline_plan = open(os.path.join(str(baseline_plans_folder), query_id, variation_file)).read()
            variation_id = Path(variation_file).stem
            if plan != baseline_plan:
                logger.info(f"[{query_id}][{variation_id}] is different for "
                            f"{optimizer.to_string()} and {baseline_optimizer.to_string()}")
                differentiating_queries[query_id].append(variation_id)

    index_folder = os.path.join(INDEX_ROOT, test_case.benchmark)
    if not os.path.exists(index_folder):
        os.mkdir(index_folder)

    with open(os.path.join(str(index_folder), optimizer.to_string() + ".json"), 'w') as f:
        json.dump(differentiating_queries, f, indent=4)


identify_differentiating_queries(optimizer=TS, baseline_optimizer=OG, test_case=TPCH)
