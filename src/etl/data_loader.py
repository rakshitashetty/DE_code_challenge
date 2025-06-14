from pyspark.sql import SparkSession, DataFrame


def load_sales_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def load_products_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def load_stores_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
