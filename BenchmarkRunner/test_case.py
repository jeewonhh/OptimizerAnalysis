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
import logging

from optimizer import *
import motherduck

MD_PREFIX = "md:"
QUERY_ROOT = "/home/ubuntu/Analysis/Data/permuted_queries"
INDEX_ROOT = "/home/ubuntu/Analysis/Indexes"


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
    message: str
    # benchmark: str
    query_id: str
    variation_id: int
    # optimizer: Optimizer
    # start_time: float
    duration: float
    status: QueryRunStatus


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
        
        logging.INFO(f"Execution of [{self.query_id}][{self.variation_id}] took {sw.time} seconds [{query_status.name}].")
        return QueryResult(
            message=error_message,
            # benchmark=self.benchmark,
            query_id=self.query_id,
            variation_id=self.variation_id,
            # optimizer=None,
            # start_time=datetime.datetime.fromtimestamp(
            #         sw.start, tz=datetime.timezone.utc
            # ),
            duration=sw.time,
            status=query_status,
        )

    @classmethod
    def from_query_info(
        cls,
        # benchmark: str,
        query_id: str,
        variation_id: int
    ):
        """ eg. /tpch/queries/q01/1.sql """
        path = os.path.join(QUERY_ROOT, benchmark, "queries", query_id, str(variation_id) + ".sql") 
        return cls(
            # benchmark=benchmark,
            query_id=query_id,
            variation_id=variation_id,
            query_text=open(path).read()
        )

    @classmethod
    def from_file(
        cls,
        path: str
    ):
        """ eg. /tpch/queries/q01/1.sql """
        pattern = r"([^/]+)/([^/]+)/([^/.]+)\.sql$"
        match = re.search(pattern, path)
        return cls(
            # benchmark=match.group(1),
            query_id=match.group(2),
            variation_id=match.group(3),
            query_text=open(path).read()
        )


@dataclass
class TestCase:
    benchmark: str
    explain: bool
    path: str = field(init=False)
    raise_on_error: Optional[bool] = False

    def __post_init__(self):
        self.path: str = os.path.join(QUERY_ROOT,
                                      self.benchmark,
                                      "explain" if self.explain else "queries")

    '''
    returns a list of QueryVariations for a given query_id
    '''
    def _collect_variations_of_query(self, query_id) -> List[QueryVariation]:
        ''' returns a list of variation ids '''
        def _expand_dir(path) -> List[int]:
            if path.endswith(".sql"):
                return [(None, path)]
            elif os.path.isdir(path):
                return [
                    int(file_name.strip(".sql"))
                    for file_name in os.listdir(path)
                    if file_name.endswith(".sql")
                ]
            else:  # not valid sql file or path
                raise ValueError(f"{path} is not a valid directory or .sql file")
        
        all_queries: List[QueryVariation] = [
            QueryVariation.from_query_info(self.benchmark, query_id, variation_id)
            for variation_id in _expand_dir(os.path.join(self.path, query_id))
        ]

        return all_queries

    
    def run(self, optimizer: Optimizer):
        all_results: List[QueryResult] = []

        try:
            self._run(all_results)
        # ruff: noqa: E722
        except Exception as e:
            if self.raise_on_error:
                raise e
            all_results.append(
                QueryResult(
                    message="Error loading test case",
                    # benchmark=self.benchmark,
                    # query_id=None,
                    # variation_id=None,
                    # optimizer=optimizer,
                    # start_time=datetime.datetime.now(datetime.timezone.utc),
                    duration=float(0),
                    status=QueryRunStatus.FAILED
                )
            )
        return all_results
            
    def _run(self, all_results: List[QueryResult]):
        conn = motherduck.connect(MD_PREFIX)
        motherduck.attach(conn, "~/local_v113.db", "local")

        query_ids = [file_name for file_name in os.listdir(self.path)]
        for query_id in ['q01']:
            print(query_id)
            all_variations = self._collect_variations_of_query(query_id)
            with stopwatch() as sw:
                for variation in all_variations:
                    result = variation.run(conn)
                    all_results.append(result)


