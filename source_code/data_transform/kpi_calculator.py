from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, round, sum, year

# Set up logging
from source_code.utils.logger import get_logger

logger = get_logger(__name__)


class Analysis:
    """
    A utility class containing static methods
    to compute business KPIs from enriched datasets.

    Methods:
        compute_sales_insight(df)
        compute_revenue_insight(df)
    """

    @staticmethod
    def compute_revenue_insight(df: DataFrame) -> DataFrame:
        """
        Calculate total revenue per store and product category.

        Parameters:
            df (DataFrame): Enriched DataFrame with sales and product info.

        Returns:
            DataFrame: store_id, category, total_revenue
        """
        try:
            logger.info("Starting revenue insight computation.")
            result_df = (
                df.withColumn("revenue", col("quantity") * col("price"))
                .groupBy("store_id", "category")
                .agg(round(sum("revenue"), 2).alias("total_revenue"))
            )
            logger.info(
                "Revenue insight computation completed. "
                "Resulting DataFrame has %d rows and %d columns.",
                result_df.count(),
                len(result_df.columns),
            )
            return result_df
        except Exception as e:
            logger.error("Failed to compute revenue insight: %s", str(e))
            raise  # Re-raise the exception after logging it

    @staticmethod
    def compute_sales_insight(df: DataFrame) -> DataFrame:
        """
        Calculate the total quantity sold per category per month.

        Parameters:
            df (DataFrame): Enriched DataFrame with sales and product info.

        Returns:
            DataFrame: year, month, category, total_quantity_sold
        """
        try:
            logger.info("Starting sales insight computation.")
            result_df = (
                df.withColumn("year", year("transaction_date"))
                .withColumn("month", month("transaction_date"))
                .groupBy("year", "month", "category")
                .agg(sum("quantity").alias("total_quantity_sold"))
            )
            logger.info(
                "Sales insight computation completed. "
                "Resulting DataFrame has %d rows and %d columns.",
                result_df.count(),
                len(result_df.columns),
            )
            return result_df
        except Exception as e:
            logger.error("Failed to compute sales insight: %s", str(e))
            raise  # Re-raise the exception after logging it
