import duckdb
from duckdb import DuckDBPyConnection
import time
import datetime
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from natsort import natsorted

from definitions import *
from optimizer import *
from logging_config import setup_logging
import logging
import motherduck


setup_logging()
logger = logging.getLogger(__name__)


class QueryRunStatus(Enum):
    FAILED = 0
    SUCCESS = 1


class stopwatch:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.time = time.time() - self.start
        self.readout = f"Time: {self.time:.6f} seconds"


@dataclass
class QueryResult:
    # benchmark: str
    # query_id: str
    variation_id: int
    # optimizer: Optimizer
    # start_time: float
    duration: float
    status: QueryRunStatus
    message: str

    def to_dict(self):
        return {
            # "benchmark": self.benchmark,
            # "query_id": self.query_id,
            "variation_id": self.variation_id,
            # "optimizer": self.optimizer,
            # "start_time": self.start_time,
            "duration": self.duration,
            "status": self.status.name,
            "message": self.message,
        }

    def to_json(self):
        return json.dumps(self.to_dict())


@dataclass
class QueryVariation:
    # benchmark: str
    query_id: str
    variation_id: int
    query_text: str
    raise_on_error: Optional[bool] = True

    def run(self, conn: DuckDBPyConnection):
        query_status = QueryRunStatus.SUCCESS
        error_message = ""
        with stopwatch() as sw:
            try:
                conn.execute(self.query_text).fetchall()
            except Exception as e:
                if self.raise_on_error:
                    raise e
                query_status = QueryRunStatus.FAILED
                error_message = str(e)

        logger.info(
            f"Execution of [{self.query_id}][{self.variation_id}] took {sw.time:.6f} seconds [{query_status.name}].")
        return QueryResult(
            message=error_message,
            # benchmark=self.benchmark,
            # query_id=self.query_id,
            variation_id=self.variation_id,
            # optimizer=None,
            # start_time=datetime.datetime.fromtimestamp(
            #         sw.start, tz=datetime.timezone.utc
            # ),
            duration=sw.time,
            status=query_status,
        )

    def run_explain(self, conn: DuckDBPyConnection):
        query_status = QueryRunStatus.SUCCESS
        try:
            explain_result = conn.execute(self.query_text).fetchone()[1]
        except Exception as e:
            if self.raise_on_error:
                raise e
            query_status = QueryRunStatus.FAILED
            error_message = str(e)

        logger.info(
            f"Explain of [{self.query_id}][{self.variation_id}] [{query_status.name}].")

        return explain_result

    @classmethod
    def from_query_info(
            cls,
            explain: bool,
            benchmark: str,
            query_id: str,
            variation_id: int
    ):
        """ eg. /tpch/queries/q07/1.sql """
        path = os.path.join(QUERY_ROOT, benchmark, "queries", query_id, str(variation_id) + ".sql")
        query_text = open(path).read()
        if explain:
            query_text = f"""
            EXPLAIN (FORMAT json) {query_text}
            """
        return cls(
            # benchmark=benchmark,
            query_id=query_id,
            variation_id=variation_id,
            query_text=query_text
        )

    @classmethod
    def from_file(
            cls,
            explain: bool,
            path: str
    ):
        """ eg. /tpch/queries/q07/1.sql """
        pattern = r"([^/]+)/([^/]+)/([^/.]+)\.sql$"
        match = re.search(pattern, path)
        return cls(
            # benchmark=match.group(1),
            query_id=match.group(2),
            variation_id=int(match.group(3)),
            query_text=open(path).read()
        )


@dataclass
class TestCase:
    benchmark: str
    path: str = field(init=False)
    raise_on_error: Optional[bool] = False

    def __post_init__(self):
        self.path: str = os.path.join(QUERY_ROOT,
                                      self.benchmark,
                                      "queries")

    @classmethod
    def from_name(cls, benchmark: str):
        return cls(
            benchmark=benchmark,
            raise_on_error=True,
        )

    '''
    returns a list of QueryVariations for a given query_id
    '''

    def _collect_variations_from_file(self, optimizer) -> Dict[str, List[int]]:
        index_file = os.path.join(INDEX_ROOT,
                                  self.benchmark,
                                  optimizer.to_string() + ".json")

        with open(index_file) as f:
            data = json.load(f)
            return data

    def _collect_variations_of_query(self, query_id) -> Dict[str, List[int]]:
        """ returns a list of variation ids """

        def _expand_dir(path) -> List[int]:
            if path.endswith(".sql"):
                return []
            elif os.path.isdir(path):
                return [
                    int(file_name.strip(".sql"))
                    for file_name in os.listdir(path)
                    if file_name.endswith(".sql")
                ]
            else:  # not valid sql file or path
                raise ValueError(f"{path} is not a valid directory or .sql file")

        all_queries: List[int] = [
            # QueryVariation.from_query_info(self.benchmark, query_id, variation_id)
            variation_id
            for variation_id in sorted(_expand_dir(os.path.join(self.path, query_id)))
        ]

        return {query_id: all_queries}

    def run(self, optimizer: Optimizer, explain: bool = False):
        try:
            if explain:
                self._run_explains(optimizer)
            else:
                all_results: Dict[str, List[QueryResult]] = {}
                self._run(optimizer, all_results)
                self.save_timed_results_as_json(optimizer, all_results)
        except Exception as e:
            if self.raise_on_error:
                raise e

    def _run_explains(self, optimizer: Optimizer):
        conn = motherduck.connect(MD_PREFIX)
        motherduck.attach(conn, "~/local_v112.db", "local")

        query_ids = natsorted([file_name for file_name in os.listdir(self.path)])
        plans_folder = os.path.join(RESULT_ROOT, self.benchmark, optimizer.to_string(), "plans")

        for query_id in query_ids:
            logger.info(f"======= Explaining query: {query_id} =======")

            #  create folder for query_id if it does not exist
            if not os.path.exists(plans_folder + "/" + query_id):
                os.makedirs(plans_folder + "/" + query_id)

            # explain all variations of query_id
            variations = self._collect_variations_of_query(query_id)
            for variation_id in variations[query_id]:
                if os.path.exists(os.path.join(str(plans_folder), query_id, str(variation_id) + ".json")):
                    # skip if plan file already exists
                    logger.info(f"Skipping [{query_id}][{variation_id}]")
                    continue
                variation = QueryVariation.from_query_info(
                    True, self.benchmark, query_id, variation_id
                )
                explain_result = variation.run_explain(conn)
                explain_path = os.path.join(str(plans_folder), query_id, str(variation_id) + ".json")
                with open(explain_path, "w") as f:
                    json.dump(json.loads(explain_result), f, indent=4)
        motherduck.detach(conn, "local")
        conn.close()

    def _run(self, optimizer: Optimizer, all_results: Dict[str, List[QueryResult]]):
        conn = motherduck.connect(MD_PREFIX)
        motherduck.attach(conn, "~/local_v112.db", "local")

        query_ids = natsorted([file_name for file_name in os.listdir(self.path)])

        variations = self._collect_variations_from_file(optimizer)

        for query_id in query_ids:
            logger.info(f"======= Executing query: {query_id} =======")
            results_for_query = []
            for variation_id in variations[query_id]:
                variation = QueryVariation.from_query_info(
                    False, self.benchmark, query_id, variation_id
                )
                result = variation.run(conn)
                results_for_query.append(result)

            all_results[query_id] = results_for_query

        conn.close()

    def save_timed_results_as_json(self, optimizer: Optimizer, all_results: Dict[str, List[QueryResult]]):
        results_folder = os.path.join(RESULT_ROOT,
                                      self.benchmark,
                                      optimizer.to_string(),
                                      "timed_results"
                                      )
        if not os.path.exists(results_folder):
            os.makedirs(results_folder)
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        result_path = os.path.join(str(results_folder), timestamp + ".json")

        with open(result_path, "w") as f:
            json.dump(all_results, f, default=lambda o: o.to_dict(), indent=4)
