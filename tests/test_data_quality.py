import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("chispa-test").master("local[*]").getOrCreate()


def test_null_removal(spark):
    data = [(1, "A"), (2, None), (3, "C")]
    expected_data = [(1, "A"), (3, "C")]
    input_df = spark.createDataFrame(data, ["id", "name"])
    expected_df = spark.createDataFrame(expected_data, ["id", "name"])
    cleaned_df = input_df.filter("name IS NOT NULL")
    assert_df_equality(cleaned_df, expected_df, ignore_row_order=True)


def test_duplicate_removal(spark):
    data = [(1, "A"), (2, "B"), (2, "B"), (3, "C")]
    expected_data = [(1, "A"), (2, "B"), (3, "C")]
    input_df = spark.createDataFrame(data, ["id", "name"])
    expected_df = spark.createDataFrame(expected_data, ["id", "name"])
    deduped_df = input_df.dropDuplicates()
    assert_df_equality(deduped_df, expected_df, ignore_row_order=True)


def test_schema_enforcement(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, schema=schema)
    assert df.schema == schema
