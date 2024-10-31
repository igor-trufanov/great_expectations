from dataclasses import dataclass
from typing import Mapping

import pandas as pd

from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig


@dataclass(frozen=True)
class TestConfig:
    data_source_config: DataSourceTestConfig
    data: pd.DataFrame
    extra_data: Mapping[str, pd.DataFrame]


@dataclass(frozen=True)
class CachableDataFrame:
    """A cachable version of a DataFrame object.

    Dataframes can't be cached since they are mutable.
    This converts it to tuples for caching, and then maps them back to DataFrames when needed.
    """

    data_tuple: tuple
    columns: tuple[str, ...]
    index: tuple

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame):
        data_tuple = tuple(map(tuple, df.values))
        columns = tuple(df.columns.tolist())
        index = tuple(df.index.tolist())
        return cls(data_tuple, columns, index)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(list(self.data_tuple), columns=self.columns, index=self.index)


@dataclass(frozen=True)
class CacheableTestConfig:
    data: CachableDataFrame
    extra_data: Mapping[str, CachableDataFrame]
    data_source_config: DataSourceTestConfig

    @classmethod
    def from_test_config(cls, test_config: TestConfig):
        return cls(
            data_source_config=test_config.data_source_config,
            data=CachableDataFrame.from_dataframe(test_config.data),
            extra_data={
                k: CachableDataFrame.from_dataframe(v) for k, v in test_config.extra_data.items()
            },
        )

    def to_test_config(self) -> TestConfig:
        return TestConfig(
            data_source_config=self.data_source_config,
            data=self.data.to_dataframe(),
            extra_data={k: v.to_dataframe() for k, v in self.extra_data.items()},
        )


# Example DataFrame
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})


# Convert DataFrame to a tuple representation
def dataframe_to_tuple(df):
    data_tuple = tuple(map(tuple, df.values))
    columns = df.columns.tolist()
    index = df.index.tolist()
    return data_tuple, columns, index


# Convert tuple representation back to DataFrame
def tuple_to_dataframe(data_tuple, columns, index):
    return pd.DataFrame(list(data_tuple), columns=columns, index=index)


# Example usage
data_tuple, columns, index = dataframe_to_tuple(df)
df_reconstructed = tuple_to_dataframe(data_tuple, columns, index)
