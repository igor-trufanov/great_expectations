from __future__ import annotations

import random
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Dict, Generic, Optional, Type, TypeVar, Union

from typing_extensions import override

import great_expectations as gx
from great_expectations.compatibility.sqlalchemy import (
    Column,
    MetaData,
    Table,
    TypeEngine,
    create_engine,
    insert,
)
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch

if TYPE_CHECKING:
    import pandas as pd
    import pytest
    from pytest import FixtureRequest

_ColumnTypes = TypeVar("_ColumnTypes")


@dataclass(frozen=True)
class DataSourceTestConfig(ABC, Generic[_ColumnTypes]):
    name: Optional[str] = None
    column_types: Union[Dict[str, _ColumnTypes], None] = None

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
    def create_batch_setup(self, data: pd.DataFrame, request: FixtureRequest) -> BatchTestSetup:
        """Create a batch setup object for this data source."""

    @property
    def test_id(self) -> str:
        parts: list[Optional[str]] = [self.label, self.name]
        non_null_parts = [p for p in parts if p is not None]

        return "-".join(non_null_parts)


_ConfigT = TypeVar("_ConfigT", bound=DataSourceTestConfig)


class BatchTestSetup(ABC, Generic[_ConfigT]):
    """ABC for classes that set up and tear down batches."""

    def __init__(self, config: _ConfigT, data: pd.DataFrame) -> None:
        self.config = config
        self.data = data

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
    def _context(self) -> AbstractDataContext:
        return gx.get_context(mode="ephemeral")


class SQLBatchTestSetup(BatchTestSetup, ABC, Generic[_ConfigT]):
    @property
    @abstractmethod
    def connection_string(self) -> str:
        """Connection string used to connect to SQL backend."""

    @property
    @abstractmethod
    def schema(self) -> Union[str, None]:
        """Schema -- if any -- to use when connecting to SQL backend."""

    @property
    @abstractmethod
    def inferrable_types_lookup(self) -> Dict[Type, TypeEngine]:
        """Dict of Python type keys mapped to SQL dialect-specific SqlAlchemy types."""

    def __init__(self, config: _ConfigT, data: pd.DataFrame) -> None:
        self.table_name = f"{config.label}_expectation_test_table_{self._random_resource_name()}"
        self.engine = create_engine(url=self.connection_string)
        self.metadata = MetaData()
        self.table: Union[Table, None] = None
        super().__init__(config, data)

    def get_column_types(self) -> Dict[str, TypeEngine]:
        column_types = self.infer_column_types()
        # prefer explicit types if they're provided
        column_types.update(self.config.column_types or {})
        untyped_columns = set(self.data.columns) - set(column_types.keys())
        if untyped_columns:
            config_class_name = self.config.__class__.__name__
            message = (
                f"Unable to infer types for the following column(s): "
                f"{', '.join(untyped_columns)}. \n"
                f"Please provide the missing types as the `column_types` "
                f"parameter when \ninstantiating {config_class_name}."
            )
            raise RuntimeError(message)
        return column_types

    def infer_column_types(self) -> Dict[str, TypeEngine]:
        inferred_column_types: Dict[str, TypeEngine] = {}
        for column, value_list in self.data.to_dict("list").items():
            python_type = type(value_list[0])
            inferred_type = self.inferrable_types_lookup.get(python_type)
            if inferred_type:
                inferred_column_types[str(column)] = inferred_type
        return inferred_column_types

    @override
    def setup(self) -> None:
        columns = [Column(name, type) for name, type in self.get_column_types().items()]
        self.table = Table(self.table_name, self.metadata, *columns, schema=self.schema)
        self.metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            # pd.DataFrame(...).to_dict("index") returns a dictionary where the keys are the row
            # index and the values are a dict of column names mapped to column values.
            # Then we pass that list of dicts in as parameters to our insert statement.
            #   INSERT INTO test_table (my_int_column, my_str_column) VALUES (?, ?)
            #   [...] [('1', 'foo'), ('2', 'bar')]
            conn.execute(insert(self.table), list(self.data.to_dict("index").values()))

    @override
    def teardown(self) -> None:
        if self.table is not None:
            self.table.drop(self.engine)
