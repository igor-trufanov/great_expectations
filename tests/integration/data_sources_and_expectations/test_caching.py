from typing import Mapping

import pandas as pd
import pytest
import sqlalchemy.dialects.postgresql as POSTGRESQL_TYPES

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
)

# @parameterize_batch_for_data_sources(
#     data_source_configs=[
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(
#             column_types={"a": POSTGRESQL_TYPES.INTEGER, "b": POSTGRESQL_TYPES.INTEGER}
#         ),
#     ],
#     data=pd.DataFrame({"a": [1, 2]}),
# )
# def test_expect_column_min_to_be_between(batch_for_datasource) -> None:
#     expectation = gxe.ExpectColumnMinToBeBetween(column="a", min_value=1, max_value=1)
#     result = batch_for_datasource.validate(expectation)
#     assert result.success


# @parameterize_batch_for_data_sources(
#     data_source_configs=[
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#         PostgreSQLDatasourceTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
#     ],
#     data=pd.DataFrame({"a": [1, 2]}),
# )
# def test_more(batch_for_datasource) -> None:
#     expectation = gxe.ExpectColumnMinToBeBetween(column="a", min_value=1, max_value=1)
#     result = batch_for_datasource.validate(expectation)
#     assert result.success


setup_count = 0
teardown_count = 0


class DummyTestConfig(DataSourceTestConfig):
    @property
    @override
    def label(self) -> str:
        return "test"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.unit

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
    ) -> BatchTestSetup:
        return DummyBatchTestSetup(data=data, config=self, extra_data=extra_data)


class DummyBatchTestSetup(BatchTestSetup):
    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self.context.data_sources.add_pandas(name)
            .add_dataframe_asset(name)
            .add_batch_definition_whole_dataframe(name)
            .get_batch(batch_parameters={"dataframe": self.data})
        )

    @override
    def setup(self) -> None:
        global setup_count  # noqa: PLW0603
        if setup_count:
            assert False, "Setup is not being cached"
        setup_count += 1

    @override
    def teardown(self) -> None:
        global teardown_count  # noqa: PLW0603
        if teardown_count:
            assert False, "Teardown is not being cached"
        teardown_count -= 1


@parameterize_batch_for_data_sources(
    data_source_configs=[
        DummyTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
        DummyTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
    ],
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_caching_within_a_test(batch_for_datasource) -> None:
    # This should fail in setup or teardown if the setup and teardown are not being cached
    ...


@parameterize_batch_for_data_sources(
    data_source_configs=[
        DummyTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
        DummyTestConfig(column_types={"a": POSTGRESQL_TYPES.INTEGER}),
    ],
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_caching_across_tests(batch_for_datasource) -> None:
    # This should fail in setup or teardown if the setup and teardown are not being cached
    ...
