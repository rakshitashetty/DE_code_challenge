from pyspark.sql import DataFrame
from typing import List, Optional

def save_output(df: DataFrame, output_path: str, file_type: str, partition_columns: Optional[List[str]] = None) -> None:

    writer = df.write.mode("overwrite")

    if file_type == "csv":
        writer.option("header", True).csv(output_path)
    elif file_type == "parquet":
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(output_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
