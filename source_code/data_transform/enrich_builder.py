from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_price(price: float) -> str | None:
    if price is None:
        return None
    elif price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    else:
        return "High"

price_category_udf = udf(categorize_price, StringType())


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


    @staticmethod
    def enrich_data_with_udf(df):
        # Add price_category column by applying the UDF on the price column
        enriched_df = df.withColumn("price_category", price_category_udf(col("price")))
        return enriched_df