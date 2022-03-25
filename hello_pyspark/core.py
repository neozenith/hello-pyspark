# Third Party
from pyspark.sql import DataFrame, SparkSession


def my_spark_function(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Pipeline step to transform DataFrame in, to DataFrame out."""
    return df
