# Standard Library
import shutil
from pathlib import Path

# Third Party
import pytest
from delta import DeltaTable
from pyspark.sql.types import (  # Row,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.mark.spark
def test_spark_delta_transform(spark, spark_logger):
    # Given
    schema_mapping = {
        "id": IntegerType(),
        "name": StringType(),
        "created_at": TimestampType(),
        "salary": DecimalType(13, 2),
    }

    schema = StructType([StructField(col_name, datatype, True) for col_name, datatype in schema_mapping.items()])

    location = "./tmp/delta-tables"
    partition_by = ["name"]
    database_name = "bronze"
    table_name = "list_of_names"
    qualified_table_name = f"{database_name}.{table_name}"

    shutil.rmtree("spark-warehouse" / Path(location), ignore_errors=True)

    df = spark.createDataFrame([], schema)
    df.show()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{location}'")

    # When
    # OPTION #1: Save an empty dataframe with the specified schema
    #  df.write.format("delta").partitionBy(*partition_by).saveAsTable(qualified_table_name)

    # OPTION #2: Use the DeltaTableBuilder
    # fmt: off
    (DeltaTable.createOrReplace(spark)
        .tableName(qualified_table_name)
        .addColumns(df.schema)
        .partitionedBy(*partition_by)
        .execute())
    # fmt: on

    # Then
    df2 = spark.sql(f"SELECT * FROM {qualified_table_name}")
    df2.show()

    # Cleanup
    #  shutil.rmtree("spark-warehouse" / Path(location), ignore_errors=True)
