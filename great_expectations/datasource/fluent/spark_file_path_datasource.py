from __future__ import annotations

from typing import (
    ClassVar,
    Sequence,
)

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.data_asset.path.spark.spark_asset import (
    SPARK_PATH_ASSET_TYPES,
    SPARK_PATH_ASSET_UNION,
)
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,  # noqa: TCH001  # pydantic requires this type at runtime
)


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[Sequence[type[DataAsset]]] = SPARK_PATH_ASSET_TYPES

    # instance attributes
    assets: list[SPARK_PATH_ASSET_UNION] = []
