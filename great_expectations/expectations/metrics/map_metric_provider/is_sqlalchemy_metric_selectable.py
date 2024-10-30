from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from great_expectations.expectations.metrics import MetaMetricProvider

logger = logging.getLogger(__name__)

SQLALCHEMY_SELECTABLE_METRICS: set[str] = {
    "compound_columns.count",
    "compound_columns.unique",
}


def _is_sqlalchemy_metric_selectable(
    map_metric_provider: MetaMetricProvider,
) -> bool:
    """
    :param map_metric_provider: object of type "MapMetricProvider", whose SQLAlchemy implementation is inspected
    :return: boolean indicating whether or not the returned value of a method implementing the metric resolves all
    columns -- hence the caller must not use "select_from" clause as part of its own SQLAlchemy query; otherwise an
    unwanted selectable (e.g., table) will be added to "FROM", leading to duplicated and/or erroneous results.
    """  # noqa: E501
    # noinspection PyUnresolvedReferences
    return (
        hasattr(map_metric_provider, "condition_metric_name")
        and map_metric_provider.condition_metric_name in SQLALCHEMY_SELECTABLE_METRICS
    ) or (
        hasattr(map_metric_provider, "function_metric_name")
        and map_metric_provider.function_metric_name in SQLALCHEMY_SELECTABLE_METRICS
    )
