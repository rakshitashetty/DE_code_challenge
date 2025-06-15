from pyspark.sql import DataFrame

def save_output(df: DataFrame, output_path: str, file_type: str = "csv") -> None:
    if file_type == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_type == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
