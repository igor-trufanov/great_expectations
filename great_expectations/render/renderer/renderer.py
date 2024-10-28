from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from typing_extensions import ParamSpec

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult

P = ParamSpec("P")
T = TypeVar("T")


def renderer(renderer_type: str, **kwargs) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def wrapper(renderer_fn: Callable[P, T]) -> Callable[P, T]:
        @wraps(renderer_fn)
        def inner_func(*args: P.args, **kwargs: P.kwargs):
            return renderer_fn(*args, **kwargs)

        inner_func._renderer_type = renderer_type  # type: ignore[attr-defined]
        inner_func._renderer_definition_kwargs = kwargs  # type: ignore[attr-defined]
        return inner_func

    return wrapper


class Renderer:
    """A convenience class to provide an explicit mechanism to instantiate any Renderer."""

    @override
    def __eq__(self, other: object) -> bool:
        # Renderers do not have any state, so they are equal if they are the same class
        return type(self) is type(other)

    def serialize(self) -> dict:
        # Necessary to enable proper serialization within an Action (and additionally, within a Checkpoint)  # noqa: E501
        # TODO: Renderers should be ported over to Pydantic to prevent this fork in logic
        return {
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }

    @classmethod
    def _get_expectation_type(cls, ge_object):
        if isinstance(ge_object, ExpectationConfiguration):
            return ge_object.type

        elif isinstance(ge_object, ExpectationValidationResult):
            # This is a validation
            return ge_object.expectation_config.type

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr.expectation_config.type == type_:
                return evr

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_all_evrs_by_type(cls, evrs, type_, column_=None):
        ret = []
        for evr in evrs:
            if evr.expectation_config.type == type_ and (
                not column_ or column_ == evr.expectation_config.kwargs.get("column")
            ):
                ret.append(evr)

        return ret

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _get_column_list_from_evrs(cls, evrs):
        """
        Get list of column names.

        If expect_table_columns_to_match_ordered_list EVR is present, use it as the list, including the order.

        Otherwise, get the list of all columns mentioned in the expectations and order it alphabetically.

        :param evrs:
        :return: list of columns with best effort sorting
        """  # noqa: E501
        evrs_ = evrs if isinstance(evrs, list) else evrs.results

        expect_table_columns_to_match_ordered_list_evr = cls._find_evr_by_type(
            evrs_, "expect_table_columns_to_match_ordered_list"
        )
        # Group EVRs by column
        sorted_columns = sorted(
            list(
                {
                    evr.expectation_config.kwargs["column"]
                    for evr in evrs_
                    if "column" in evr.expectation_config.kwargs
                }
            )
        )

        if expect_table_columns_to_match_ordered_list_evr:
            ordered_columns = expect_table_columns_to_match_ordered_list_evr.result[
                "observed_value"
            ]
        else:
            ordered_columns = []

        # only return ordered columns from expect_table_columns_to_match_ordered_list evr if they match set of column  # noqa: E501
        # names from entire evr
        if set(sorted_columns) == set(ordered_columns):
            return ordered_columns
        else:
            return sorted_columns

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _group_evrs_by_column(cls, validation_results):
        columns = {}
        for evr in validation_results.results:
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-level Expectations"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        return columns

    def render(self, checkpoint_result: CheckpointResult) -> Any:
        """
        Render interface method.
        """
        raise NotImplementedError
