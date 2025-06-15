from pyspark.sql import SparkSession

from src.utils.config_loader import load_yaml_config
from src.utils.io_utils import save_output
# from src.utils.logger import get_logger

from src.data_prep.generic_load_job import GenericETLJob
from src.data_transform.transform_job import DataTransformer
from src.data_transform.analysis_job import Analysis



# logger = get_logger(__name__)

def main(config_path):
    config = load_yaml_config(config_path)
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    # Phase 1: Data Preparation
    for dataset_name in ['products', 'stores', 'sales']:
        dataset_config = config[dataset_name]
        etl_job = GenericETLJob(spark, dataset_config)
        etl_job.load()

    # Phase 2: Data transformation
    transformer = DataTransformer(spark, config)
    transformer.load_data()

    enriched_df = transformer.enrich_data()
    revenue_df = Analysis.compute_revenue_insight(enriched_df)
    sales_df = Analysis.compute_sales_insight(enriched_df)

    save_output(enriched_df, config["output_paths"]["enriched"], file_type="parquet")
    save_output(revenue_df, config["output_paths"]["revenue"], file_type="csv")
    save_output(sales_df, config["output_paths"]["sales"], file_type="csv")

if __name__ == "__main__":
    import sys
    main(sys.argv[1])