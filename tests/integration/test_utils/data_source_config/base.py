from __future__ import annotations

import random
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Hashable, Mapping, Optional, TypeVar

import pandas as pd

import great_expectations as gx
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch

if TYPE_CHECKING:
    import pytest
    from pytest import FixtureRequest

_ColumnTypes = TypeVar("_ColumnTypes")


@dataclass(frozen=True)
class DataSourceTestConfig(ABC, Generic[_ColumnTypes]):
    name: Optional[str] = None
    column_types: Optional[Mapping[str, _ColumnTypes]] = None
    extra_assets: Optional[Mapping[str, Mapping[str, _ColumnTypes]]] = None

    @property
    @abstractmethod
    def label(self) -> str:
        """Label that will show up in test name."""
        ...

    @property
    @abstractmethod
    def pytest_mark(self) -> pytest.MarkDecorator:
        """Mark for pytest"""
        ...

    @abstractmethod
    def create_batch_setup(
        self,
        request: FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
    ) -> BatchTestSetup:
        """Create a batch setup object for this data source."""

    @property
    def test_id(self) -> str:
        parts: list[Optional[str]] = [self.label, self.name]
        non_null_parts = [p for p in parts if p is not None]

        return "-".join(non_null_parts)

    @override
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, DataSourceTestConfig):
            return False
        return all(
            [
                super().__eq__(value),
                self.label == value.label,
                self.pytest_mark == value.pytest_mark,
            ]
        )

    @override
    def __hash__(self) -> int:
        assets_dict = self.extra_assets
        hashable_col_types = dict_to_tuple(self.column_types) if self.column_types else None
        hashable_extra_assets = (
            dict_to_tuple({k: dict_to_tuple(assets_dict[k]) for k in sorted(assets_dict)})
            if assets_dict
            else None
        )
        return hash(
            (
                self.__class__.name,
                self.test_id,
                hashable_col_types,
                hashable_extra_assets,
            )
        )


_ConfigT = TypeVar("_ConfigT", bound=DataSourceTestConfig)


class BatchTestSetup(ABC, Generic[_ConfigT]):
    """ABC for classes that set up and tear down batches."""

    def __init__(
        self,
        config: _ConfigT,
        data: pd.DataFrame,
        extra_data: Optional[Mapping[str, pd.DataFrame]] = None,
    ) -> None:
        self.config = config
        self.data = data
        self.extra_data = extra_data or {}

    @abstractmethod
    def make_batch(self) -> Batch: ...

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None: ...

    @staticmethod
    def _random_resource_name() -> str:
        return "".join(random.choices(string.ascii_lowercase, k=10))

    @cached_property
    def context(self) -> AbstractDataContext:
        return gx.get_context(mode="ephemeral")

    def _hash_the_unhashable(self, df: pd.DataFrame) -> int:
        return hash(tuple(pd.util.hash_pandas_object(df).array))


def dict_to_tuple(d: Mapping[str, Hashable]) -> tuple[tuple[str, Hashable], ...]:
    return tuple((key, d[key]) for key in sorted(d))
