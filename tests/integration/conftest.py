from dataclasses import dataclass
from typing import Callable, Generator, Hashable, Mapping, Optional, Sequence, TypeVar

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config import DataSourceTestConfig
from tests.integration.test_utils.data_source_config.base import BatchTestSetup

_F = TypeVar("_F", bound=Callable)


@dataclass(frozen=True)
class TestConfig:
    data_source_config: DataSourceTestConfig
    data: pd.DataFrame
    extra_data: Mapping[str, pd.DataFrame]

    @override
    def __hash__(self) -> int:
        return hash(
            (
                self.__class__.__name__,
                self.data_source_config,
                self._hash_the_unhashable(self.data),
                self._dict_to_tuple(
                    {
                        k: self._hash_the_unhashable(self.extra_data[k])
                        for k in sorted(self.extra_data)
                    }
                ),
            )
        )

    @override
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, TestConfig):
            return False
        return all(
            [
                self.data_source_config == value.data_source_config,
                self.data.equals(value.data),
                self.extra_data.keys() == value.extra_data.keys(),
                all(self.extra_data[k].equals(value.extra_data[k]) for k in self.extra_data),
            ]
        )

    @staticmethod
    def _hash_the_unhashable(df: pd.DataFrame) -> int:
        return hash(tuple(pd.util.hash_pandas_object(df).array))

    @staticmethod
    def _dict_to_tuple(d: Mapping[str, Hashable]) -> tuple[tuple[str, Hashable], ...]:
        return tuple((key, d[key]) for key in sorted(d))


def parameterize_batch_for_data_sources(
    data_source_configs: Sequence[DataSourceTestConfig],
    data: pd.DataFrame,
    extra_data: Optional[Mapping[str, pd.DataFrame]] = None,
) -> Callable[[_F], _F]:
    """Test decorator that parametrizes a test function with batches for various data sources.
    This injects a `batch_for_datasource` parameter into the test function for each data source
    type.

    Args:
        data_source_configs: The data source configurations to test.
        data: Data to load into the asset
        extra_data: Mapping of {asset_name: data} to load into other assets. Only relevant for SQL
                    mutli-table expectations.


    example use:
        @parameterize_batch_for_data_sources(
            data_source_configs=[DataSourceType.FOO, DataSourceType.BAR],
            data=pd.DataFrame{"col_name": [1, 2]},
            # description="test_stuff",
        )
        def test_stuff(batch_for_datasource) -> None:
            ...
    """

    def decorator(func: _F) -> _F:
        pytest_params = [
            pytest.param(
                TestConfig(
                    data_source_config=config,
                    data=data,
                    extra_data=extra_data or {},
                ),
                id=config.test_id,
                marks=[config.pytest_mark],
            )
            for config in data_source_configs
        ]
        parameterize_decorator = pytest.mark.parametrize(
            batch_for_datasource.__name__,
            pytest_params,
            indirect=True,
        )
        return parameterize_decorator(func)

    return decorator


cached_test_configs: dict[TestConfig, BatchTestSetup] = {}


@pytest.fixture(scope="session")
def _cleanup() -> Generator[None, None, None]:
    yield
    for batch_setup in cached_test_configs.values():
        batch_setup.teardown()


@pytest.fixture
def batch_for_datasource(
    request: pytest.FixtureRequest,
    _cleanup,
) -> Generator[Batch, None, None]:
    """Fixture that yields a batch for a specific data source type.
    This must be used in conjunction with `indirect=True` to defer execution
    """
    config = request.param
    assert isinstance(config, TestConfig)

    if config not in cached_test_configs:
        batch_setup = config.data_source_config.create_batch_setup(
            request=request,
            data=config.data,
            extra_data=config.extra_data,
        )
        cached_test_configs[config] = batch_setup
        batch_setup.setup()

    batch_setup = cached_test_configs[config]
    # batch_setup = config.data_source_config.create_batch_setup(
    #     request=request,
    #     data=config.data,
    #     extra_data=config.extra_data,
    # )

    # if batch_setup not in cached_test_configs:
    #     cached_test_configs.add(batch_setup)

    yield batch_setup.make_batch()

    # cached_test_configs[batch_setup] -= 1
    # if cached_test_configs[batch_setup] == 0:
    #     batch_setup.teardown()
