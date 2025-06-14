# # import argparse
# # from src.etl.run_etl import run_etl_job
# #
# # def main():
# #     parser = argparse.ArgumentParser(description="Run Generic ETL Job with Config")
# #     parser.add_argument(
# #         "--config",
# #         required=True,
# #         help="Path to the YAML config file (e.g. config/sales_job.yaml)"
# #     )
# #     args = parser.parse_args()
# #     run_etl_job(args.config)
# #
# # if __name__ == "__main__":
# #     main()
#

from src.etl.run_etl import run_etl_job

def main(config_path: str):
    run_etl_job(config_path)


if __name__ == "__main__":
    # main('../config/sales_job.yaml')
    main('../config/stores_job.yaml')
    # main('../config/products_job.yaml')


#
# from pyspark.sql import SparkSession
# from src.utils.config_loader import load_yaml_config
# # from src.utils.logger import get_logger
# from src.etl.etl_factory import get_etl_job
#
# # logger = get_logger(__name__)
#
# def main(config_path):
#     config = load_yaml_config(config_path)
#     spark = SparkSession.builder.appName("ETL").getOrCreate()
#
#     job = get_etl_job(config["file_type"], spark, config)
#     job.run()
#     # logger.info("ETL job completed.")
#
# if __name__ == "__main__":
#     import sys
#     main('../config/sales_job.yaml')
