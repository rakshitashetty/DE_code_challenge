from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, month, year, round

class Analysis:
    @staticmethod
    def compute_revenue_insight(df: DataFrame) -> DataFrame:
        """
        Calculate total revenue per store and product category.

        Parameters:
            df (DataFrame): Enriched DataFrame with sales and product info.

        Returns:
            DataFrame: store_id, category, total_revenue
        """
        return df.withColumn("revenue", col("quantity") * col("price")) \
            .groupBy("store_id", "category") \
            .agg(round(sum("revenue"), 2).alias("total_revenue"))

    @staticmethod
    def compute_sales_insight(df: DataFrame) -> DataFrame:
        """
        Calculate the total quantity sold per category per month.

        Parameters:
            df (DataFrame): Enriched DataFrame with sales and product info.

        Returns:
            DataFrame: year, month, category, total_quantity_sold
        """
        return df.withColumn("year", year("transaction_date")) \
            .withColumn("month", month("transaction_date")) \
            .groupBy("year", "month", "category") \
            .agg(sum("quantity").alias("total_quantity_sold"))

