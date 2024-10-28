from random import randint
from typing import Union

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIGINT,
    BIT,
    BOOLEAN,
    BYTEA,
    CHAR,
    CIDR,
    CITEXT,
    DATE,
    DATEMULTIRANGE,
    DATERANGE,
    DOMAIN,
    DOUBLE_PRECISION,
    ENUM,
    FLOAT,
    HSTORE,
    INET,
    INT4MULTIRANGE,
    INT4RANGE,
    INT8MULTIRANGE,
    INT8RANGE,
    INTEGER,
    INTERVAL,
    JSON,
    JSONB,
    JSONPATH,
    MACADDR,
    MACADDR8,
    MONEY,
    NUMERIC,
    NUMMULTIRANGE,
    NUMRANGE,
    OID,
    REAL,
    REGCLASS,
    REGCONFIG,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TSMULTIRANGE,
    TSQUERY,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    UUID,
    VARCHAR,
)

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
)

# Sqlalchemy follows the convention of exporting all known valid types for a given dialect
# as uppercase types from the namespace `sqlalchemy.dialects.<dialect>
PostgresColumnType = Union[
    ARRAY,
    BIGINT,
    BIT,
    BOOLEAN,
    BYTEA,
    CHAR,
    CIDR,
    CITEXT,
    DATE,
    DATEMULTIRANGE,
    DATERANGE,
    DOMAIN,
    DOUBLE_PRECISION,
    ENUM,
    FLOAT,
    HSTORE,
    INET,
    INT4MULTIRANGE,
    INT4RANGE,
    INT8MULTIRANGE,
    INT8RANGE,
    INTEGER,
    INTERVAL,
    JSON,
    JSONB,
    JSONPATH,
    MACADDR,
    MACADDR8,
    MONEY,
    NUMERIC,
    NUMMULTIRANGE,
    NUMRANGE,
    OID,
    REAL,
    REGCLASS,
    REGCONFIG,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TSMULTIRANGE,
    TSQUERY,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    UUID,
    VARCHAR,
]


class PostgreSQLDatasourceTestConfig(DataSourceTestConfig):
    column_map: dict[str, PostgresColumnType]

    def __init__(self, column_map: dict[str, PostgresColumnType]):
        # todo: make column map optional and add type inference
        self.column_map = column_map

    @property
    @override
    def label(self) -> str:
        return "postgresql"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.postgresql

    @override
    def create_batch_setup(
        self, data: pd.DataFrame, request: pytest.FixtureRequest
    ) -> BatchTestSetup:
        return PostgresBatchTestSetup(
            data=data,
            config=self,
        )


class PostgresBatchTestSetup(BatchTestSetup[PostgreSQLDatasourceTestConfig]):
    def __init__(
        self,
        config: PostgreSQLDatasourceTestConfig,
        data: pd.DataFrame,
    ) -> None:
        self.table_name = f"postgres_expectation_test_table_{randint(0, 1000000)}"
        self.connection_string = "postgresql+psycopg2://postgres@localhost:5432/test_ci"
        self.engine = create_engine(url=self.connection_string)
        self.metadata = MetaData()
        self.table: Union[Table, None] = None
        self.schema = "public"
        super().__init__(config=config, data=data)

    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self._context.data_sources.add_postgres(
                name=name, connection_string=self.connection_string
            )
            .add_table_asset(
                name=name,
                table_name=self.table_name,
                schema_name=self.schema,
            )
            .add_batch_definition_whole_table(name=name)
            .get_batch()
        )

    @override
    def setup(self) -> None:
        columns = [
            Column(name, column_type) for name, column_type in self.config.column_map.items()
        ]
        self.table = Table(self.table_name, self.metadata, *columns, schema=self.schema)
        self.metadata.create_all(self.engine)
        with self.engine.connect() as conn:
            for row_dict in self.data.to_dict("index").values():
                insert_stmt = insert(self.table).values(row_dict)
                conn.execute(insert_stmt)
            conn.commit()

    @override
    def teardown(self) -> None:
        if self.table is not None:
            self.table.drop(self.engine)


if __name__ == "__main__":
    engine = create_engine(url="postgresql://postgres@localhost/test_ci")
    metadata_obj = MetaData()
    user = Table(
        "user",
        metadata_obj,
        Column("user_id", Integer, primary_key=True),
    )
    metadata_obj.create_all(engine)
    user.drop(engine)
