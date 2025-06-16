from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType



# Set up logging
from source_code.utils.logger import get_logger
logger = get_logger(__name__)

# Categorizes price into buckets: Low (<20), Medium (20–100), High (>100), or None if price is null
def categorize_price(price: float) -> str | None:
    """
    Categorizes a numerical price value into "Low", "Medium", or "High" based on predefined thresholds.

    Parameter:
        price: Price of each transaction

    Returns:
         Either category value or None for null input.
    """

    if price is None:
        return None
    elif price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    else:
        return "High"

price_category_udf = udf(categorize_price, StringType())


# Class to load and enrich datasets for sales analysis using PySpark
class DataTransformer:
    """
    A class to load and enrich sales, products, and store datasets using PySpark.
    It performs joins to create a unified dataset and includes functionality to derive additional features using UDFs.
    """
    def __init__(self, spark: SparkSession, config):
        """
        Initializes the DataTransformer with a Spark session and configuration dictionary containing paths to cleaned data.

        Parameters:
            spark (SparkSession): Active Spark session.
            config (dict): Configuration dictionary for the ETL job.
        """
        self.products_df = None
        self.sales_df = None
        self.stores_df = None
        self.spark = spark
        self.config = config

        logger.info("Initialized DataTransformer with configuration: %s", config)

    # Loads the clean sales, products, and stores data from Parquet files using config paths
    def load_data(self):
        """
        Loads cleaned Parquet files for sales, products, and stores into Spark DataFrames using paths specified in the configuration.
        :return:
        """
        try:
            logger.info("Loading sales data from %s", self.config['clean_paths']['sales'])
            self.sales_df = self.spark.read.parquet(self.config['clean_paths']['sales'])
            logger.info("Successfully loaded sales data with %d rows and %d columns", self.sales_df.count(),
                        len(self.sales_df.columns))

            logger.info("Loading products data from %s", self.config['clean_paths']['products'])
            self.products_df = self.spark.read.parquet(self.config['clean_paths']['products'])
            logger.info("Successfully loaded products data with %d rows and %d columns", self.products_df.count(),
                        len(self.products_df.columns))

            logger.info("Loading stores data from %s", self.config['clean_paths']['stores'])
            self.stores_df = self.spark.read.parquet(self.config['clean_paths']['stores'])
            logger.info("Successfully loaded stores data with %d rows and %d columns", self.stores_df.count(),
                        len(self.stores_df.columns))

        except Exception as e:
            logger.error("Failed to load data: %s", e)
            raise  # Re-raise the exception after logging

    def enrich_data(self) -> DataFrame:
        """
        Performs left joins between sales, products, and stores DataFrames on their respective keys to create an enriched dataset.
        Returns:
            enriched_df: Dataframe.
        """
        try:
            logger.info("Enriching data by joining sales, products, and stores.")
            enriched_df = self.sales_df \
                .join(self.products_df, on="product_id", how="left") \
                .join(self.stores_df, on="store_id", how="left")

            logger.info("Successfully enriched data with %d rows and %d columns", enriched_df.count(),
                        len(enriched_df.columns))
            return enriched_df
        except Exception as e:
            logger.error("Failed to enrich data: %s", e)
            raise  # Re-raise the exception after logging

    @staticmethod
    def enrich_data_with_udf(df):
        """
        Adds a price_category column to the DataFrame by applying a UDF that categorizes product prices into predefined buckets.

        Parameter:
            df (Dataframe): Input is the enriched data set from the previous function

        Returns:
            enriched_df: Output is the entire input along with the price_category


        """

        try:
            logger.info("Adding price category using UDF.")
            enriched_df = df.withColumn("price_category", price_category_udf(col("price")))
            logger.info("Successfully added price_category column.")
            return enriched_df
        except Exception as e:
            logger.error("Failed to apply UDF for price categorization: %s", e)
            raise  # Re-raise the exception after logging