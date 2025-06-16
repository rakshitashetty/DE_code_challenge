from pyspark.sql import DataFrame
from typing import List, Optional

# Set up logging
from source_code.utils.logger import get_logger

logger = get_logger(__name__)


def save_output(
    df: DataFrame,
    output_path: str,
    file_type: str,
    partition_columns: Optional[List[str]] = None,
) -> None:
    """
    Saves a DataFrame to disk in the specified format.

    Args:
        df (DataFrame): The DataFrame to save.
        output_path (str): Destination path.
        file_type (str): File format (e.g., 'csv', 'parquet').
        partition_columns: If data is to be partitioned, then mention the columns
    """
    logger.info(f"Started saving DataFrame to {output_path} in {file_type} format.")

    try:
        writer = df.write.mode("overwrite")

        if file_type == "csv":
            logger.info("Saving as CSV with header option enabled.")
            writer.option("header", True).csv(output_path)
        elif file_type == "parquet":
            logger.info("Saving as Parquet.")
            if partition_columns:
                logger.info(f"Partitioning by columns: {', '.join(partition_columns)}.")
                writer = writer.partitionBy(*partition_columns)
            writer.parquet(output_path)
        else:
            logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        logger.info(
            f"Successfully saved the DataFrame to {output_path} in {file_type} format."
        )

    except Exception as e:
        logger.error(f"Failed to save DataFrame to {output_path} due to: {e}")
        raise  # Re-raise the exception after logging it
