from pyspark.sql import SparkSession
from datetime import datetime
import os

from source_code.utils.config_loader import load_yaml_config
from source_code.utils.file_io import save_output


from source_code.data_prep.data_loader import GenericETLJob
from source_code.data_transform.enrich_builder import DataTransformer
from source_code.data_transform.kpi_calculator import Analysis



# Set up logging
from source_code.utils.logger import get_logger
logger = get_logger(__name__)

def main(config_path):
    """
        Main entry point of the pipeline.

        Loads config, runs ingestion, transformation, and analysis.

        Args:
            config_path (str): Path to the master configuration YAML.
        """
    # Load configuration from YAML file
    config = load_yaml_config(config_path)
    logger.info("Loaded configuration from: %s", config_path)

    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    logger.info("SparkSession initialized successfully.")

    # ------------------------ Phase 1: Data Preparation ------------------------ #
    for dataset_name in ['products', 'stores', 'sales']:
        # Start ETL for each dataset
        logger.info("Starting ETL for dataset: %s", dataset_name)
        dataset_config = config[dataset_name]

        # Run generic ETL job (includes null & duplicate checks and schema enforcement)
        etl_job = GenericETLJob(spark, dataset_config)
        etl_job.load()
        logger.info("Completed loading and cleaning for dataset: %s", dataset_name)

    # ------------------------ Phase 2: Data Transformation --------------------- #
    # Load cleaned datasets
    transformer = DataTransformer(spark, config)
    logger.info("Starting data transformation phase.")
    transformer.load_data()

    # Perform joins to enrich data
    logger.info("Data enrichment started.")
    enriched_df = transformer.enrich_data()

    # Apply UDF to categorize product prices
    enriched_df=transformer.enrich_data_with_udf(enriched_df)
    logger.info("Data enrichment completed.")

    # ------------------------ Phase 3: KPI Calculation ------------------------- #
    logger.info("Computing KPI: Total Revenue by store")
    revenue_df = Analysis.compute_revenue_insight(enriched_df)

    logger.info("Computing KPI: Monthly sales insights")
    sales_df = Analysis.compute_sales_insight(enriched_df)

    # ------------------------ Phase 4: Output Writing -------------------------- #

    # Save enriched data
    logger.info("Saving enriched data to: %s", config["output_paths"]["enriched"])
    save_output(enriched_df, config["output_paths"]["enriched"], file_type="parquet", partition_columns=config["partition"]["enriched"])

    # Create timestamped KPI output folder
    basepath=config["output_paths"]["kpi"]
    rundate=datetime.now().strftime("%d%m%y")
    date_folder=os.path.join(basepath,rundate)
    logger.info("Creating KPI output folder: %s", date_folder)
    os.makedirs(date_folder,exist_ok=True)

    # Save revenue KPI
    revenue_file_name="total_revenue_by_store"
    revenue_output_path=os.path.join(date_folder,revenue_file_name)
    logger.info("Saving revenue metrics to: %s", revenue_output_path)
    save_output(revenue_df, revenue_output_path, file_type="csv")

    # Save sales insights KPI
    sales_file_name = "monthly_sales_insights"
    sales_output_path = os.path.join(date_folder, sales_file_name)
    logger.info("Saving sales metrics to: %s", sales_output_path)
    save_output(sales_df, sales_output_path, file_type="csv")

    logger.info("Pipeline execution completed successfully.")

if __name__ == "__main__":
    import sys
    main(sys.argv[1])