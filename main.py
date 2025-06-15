from pyspark.sql import SparkSession
from datetime import datetime
import os

from source_code.utils.config_loader import load_yaml_config
from source_code.utils.file_io import save_output
# from source_code.utils.logger import get_logger

from source_code.data_prep.data_loader import GenericETLJob
from source_code.data_transform.enrich_builder import DataTransformer
from source_code.data_transform.kpi_calculator import Analysis



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

    basepath=config["output_paths"]["kpi"]
    rundate=datetime.now().strftime("%d%m%y")
    date_folder=os.path.join(basepath,rundate)
    os.makedirs(date_folder,exist_ok=True)

    revenue_file_name="total_revenue_by_store"
    revenue_output_path=os.path.join(date_folder,revenue_file_name)
    save_output(revenue_df, revenue_output_path, file_type="csv")

    sales_file_name = "monthly_sales_insights"
    sales_output_path = os.path.join(date_folder, sales_file_name)
    save_output(sales_df, sales_output_path, file_type="csv")

if __name__ == "__main__":
    import sys
    main(sys.argv[1])