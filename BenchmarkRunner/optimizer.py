from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class OptimizerType(Enum):
    HEURISTIC = 1
    DP = 2


class EstimationFunction(Enum):
    CARDINALITY = 1
    TABLE_SIZE = 2
    DATA_SIZE = 3
    DATA_SIZE_SIMPLIFIED = 4


@dataclass
class Optimizer:
    type: OptimizerType
    estimation_function: Optional[EstimationFunction] = None

    def to_string(self):
        if self.type == OptimizerType.HEURISTIC:
            return self.type.name

        assert self.estimation_function

        return self.estimation_function.name
