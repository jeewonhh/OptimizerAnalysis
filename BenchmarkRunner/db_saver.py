import duckdb
from duckdb import DuckDBPyConnection
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import glob
import polars as pl

from test_case import *
from definitions import *


def save_timed_results_in_md(optimizer: Optimizer, test_case: TestCase):
    results_folder = os.path.join(RESULT_ROOT, test_case.benchmark, optimizer.to_string(), "timed_results")
    result_files = glob.glob(str(results_folder) + "/*.json")

    # combined_results: Dict[str, Dict[int, List[float]]] = {}
    combined_results = []

    for result_file in result_files:
        with open(result_file) as f:
            results = json.load(f)
            for query_id, timed_variations in results.items():
                for timed_variation in timed_variations:
                    combined_results.append({
                        "query_id": query_id,
                        "variation_id": int(timed_variation["variation_id"]),
                        "duration": float(timed_variation["duration"])
                    })

    df = pl.DataFrame(combined_results)
    df = df.group_by(["query_id", "variation_id"]).agg(
        [
            pl.col("duration").mean().alias("mean"),
            pl.col("duration").std().alias("std")
        ]
    )

    os.environ["OPTIMIZER"] = "HEURISTIC"
    conn = motherduck.connect(MD_PREFIX + "benchmark",
                              additional_config={"motherduck_token": os.getenv("TOKEN_PROD")},)

    conn.sql(f"CREATE SCHEMA IF NOT EXISTS {test_case.benchmark}")
    conn.sql(f"CREATE or replace table {test_case.benchmark}.{optimizer.to_string().lower()} AS SELECT * FROM df")

    conn.close()

