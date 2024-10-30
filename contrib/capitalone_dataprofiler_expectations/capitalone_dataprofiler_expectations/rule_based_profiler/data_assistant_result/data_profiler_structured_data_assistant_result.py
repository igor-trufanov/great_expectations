from typing import Union

from great_expectations.experimental.rule_based_profiler.altair import AltairDataTypes
from great_expectations.experimental.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


class DataProfilerStructuredDataAssistantResult(DataAssistantResult):
    """
    Note (10/18/2022): Plotting functionality is not implemented.
    """

    @property
    def metric_expectation_map(self) -> dict[Union[str, tuple[str]], str]:
        """
        A mapping is defined for which metrics to plot and their associated expectations.
        """
        return {}

    @property
    def metric_types(self) -> dict[str, AltairDataTypes]:
        """
        A mapping is defined for the Altair data type associated with each metric.
        """
        # Altair data types can be one of:
        #     - Nominal: Metric is a discrete unordered category
        #     - Ordinal: Metric is a discrete ordered quantity
        #     - Quantitative: Metric is a continuous real-valued quantity
        #     - Temporal: Metric is a time or date value
        return {}
