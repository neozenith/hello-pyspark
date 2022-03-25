# Third Party
# About pytest fixtures
# https://docs.pytest.org/en/latest/how-to/fixtures.html#how-to-fixtures
# Third Party
from delta import DeltaTable, configure_spark_with_delta_pip  # noqa
from pyspark.sql import SparkSession
from pytest import fixture


@fixture
def spark():
    """Start a local pyspark instance to test against."""
    # Setup
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    # Tear down
    spark.stop()


@fixture
def spark_logger(spark):
    # Hook into underlying log4j logger
    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.WARN)
    yield logger
