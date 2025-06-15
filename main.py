
from pyspark.sql import SparkSession
from src.utils.config_loader import load_yaml_config
# from src.utils.logger import get_logger
from src.data_prep.etl_factory import get_etl_job

# logger = get_logger(__name__)

def main(config_path):
    config = load_yaml_config(config_path)
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    job = get_etl_job(config["file_type"], spark, config)
    job.run()

    with open("config.yaml") as f:
        config = yaml.safe_load(f)


    # Phase 2: Data transformation
    transformer = DataTransformer(spark, config)
    transformer.load_data()

    enriched_df = transformer.enrich_data()
    revenue_df = transformer.compute_revenue_insight(enriched_df)
    sales_df = transformer.compute_sales_insight(enriched_df)

    transformer.save_output(enriched_df, config["output_paths"]["enriched"], file_type="parquet")
    transformer.save_output(revenue_df, config["output_paths"]["revenue"], file_type="csv")
    transformer.save_output(sales_df, config["output_paths"]["sales"], file_type="csv")

if __name__ == "__main__":
    import sys
    main(sys.argv[1])