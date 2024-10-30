from __future__ import annotations

from typing import Any, Union

import numpy as np
import pandas as pd

MetricValue = Union[
    Any,  # Encompasses deferred-query/execution plans ("SQLAlchemy" and "Spark") conditions and aggregation functions.  # noqa: E501
    list[Any],
    set[Any],
    tuple[Any, ...],
    pd.DataFrame,
    pd.Series,
    np.ndarray,
    int,
    str,
    float,
    bool,
    dict[str, Any],
]
