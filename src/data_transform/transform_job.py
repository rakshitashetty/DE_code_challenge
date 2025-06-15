from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, sum, col


class DataTransformer:
    def __init__(self, spark: SparkSession, config):
        self.products_df = None
        self.sales_df = None
        self.stores_df = None
        self.spark = spark
        self.config = config

    def load_data(self):
        self.sales_df = self.spark.read.parquet(self.config['clean_paths']['sales'])
        self.products_df = self.spark.read.parquet(self.config['clean_paths']['products'])
        self.stores_df = self.spark.read.parquet(self.config['clean_paths']['stores'])

    def enrich_data(self) -> DataFrame:
        enriched_df = self.sales_df \
            .join(self.products_df, on="product_id", how="left") \
            .join(self.stores_df, on="store_id", how="left")
        return enriched_df