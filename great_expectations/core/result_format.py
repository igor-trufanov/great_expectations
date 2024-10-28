from __future__ import annotations

import enum
from typing import Final, Literal, Union

from great_expectations._docs_decorators import public_api


@public_api
class ResultFormat(str, enum.Enum):
    """
    Responsible for configuring the level of detail for Validation Results in Data Docs.

    For example, you can return a success or failure message, a summary of observed values, a list
    of failing values, or you can add a query or a filter function that returns all failing rows.
    Typical use cases for this parameter include cleaning data and excluding Validation Result data
    in published Data Docs.

    Possible options:
    - BOOLEAN_ONLY: No result is returned; results can be evaluated based on their success param.
    - BASIC: Result contains basic justifications for success or failure.
    - SUMMARY: Result contains summary justification for success or failure.
    - COMPLETE: Result contains all available justification about success or failure.
    """

    BOOLEAN_ONLY = "BOOLEAN_ONLY"
    BASIC = "BASIC"
    COMPLETE = "COMPLETE"
    SUMMARY = "SUMMARY"


ResultFormatUnion = Union[
    ResultFormat, dict, Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
]

DEFAULT_RESULT_FORMAT: Final = ResultFormat.SUMMARY
