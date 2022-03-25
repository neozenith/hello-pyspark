# Standard Library
import shutil
from pathlib import Path

# Third Party
from pyspark.sql.types import (  # Row,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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
    df.write.format("delta").partitionBy(*partition_by).saveAsTable(qualified_table_name)

    # Then
    df2 = spark.sql(f"SELECT * FROM {qualified_table_name}")
    df2.show()

    # Cleanup
    shutil.rmtree("spark-warehouse" / Path(location), ignore_errors=True)
