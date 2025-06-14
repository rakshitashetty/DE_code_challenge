from pyspark.sql import SparkSession
from src.utils.config_loader import load_yaml_config
from src.etl.data_loader import load_sales_data, load_products_data, load_stores_data
from src.utils.date_utils import parse_multiple_date_formats

spark = SparkSession.builder.appName("GenericETL").config("spark.hadoop.io.nativeio.useLegacyFileSystem", "true").getOrCreate()


# Loader registry (maps file_type to the correct function)
loader_registry = {
    "sales": load_sales_data,
    "products": load_products_data,
    "stores": load_stores_data,
}

def run_etl_job(config_path: str):
    # Load config
    config = load_yaml_config(config_path)
    file_type = config.get("file_type")
    file_path = config.get("file_path")
    output_path = config.get("output_path")

    if file_type not in loader_registry:
        raise ValueError(f"Unsupported file_type '{file_type}'. Supported types: {list(loader_registry.keys())}")


    # Load dataset using the appropriate loader function
    df = loader_registry[file_type](spark, file_path)

    # Special handling for sales data (date parsing)
    if file_type == "sales":
        date_column = config.get("date_column")

        if not date_column:
            raise ValueError("Missing 'date_column' in config for sales data")

        df = parse_multiple_date_formats(
            df,
            input_col=date_column,
            output_col=date_column
        )

    # Write output
    #df.write.mode("overwrite").parquet(output_path)
    #df.write.mode("overwrite").format("csv").save(output_path)
    df.write.option("mode","overwrite").option("header","true").csv("../output/clean_sales.csv")
    # df.show()

    spark.stop()
