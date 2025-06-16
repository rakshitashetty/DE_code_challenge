from pyspark.sql import SparkSession
from source_code.utils.dq_validation import *


class GenericETLJob:
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the base ETL job with a Spark session and configuration.

        Parameters:
            spark (SparkSession): Active Spark session.
            config (dict): Configuration dictionary for the ETL job.
        """
        self.spark = spark
        self.config = config

    def load(self):
        print(self.config)
        input_path = self.config["input_path"]
        output_path = self.config["output_path"]
        schema_dict = self.config["schema"]


        # Read data
        df = self.spark.read.option("header", True).csv(input_path)


        # Clean nulls
        columns_for_null_check = get_columns_for_check(df, self.config['null_check']['columns'])
        df = remove_nulls(df, columns_for_null_check, self.config['null_check']['null_path'])




        # Deduplicate
        columns_for_dup_check = get_columns_for_check(df, self.config['dup_check']['columns'])
        df = remove_duplicates(df, columns_for_dup_check, self.config['dup_check']['dup_path'])

        # Schema enforcement
        df = enforce_schema(df, schema_dict)


        #Scehma validation
        mismatches = validate_schema(df, schema_dict)
        if mismatches:
            print("Mismatch")
            print(mismatches)
            print(schema_dict)
        else:
            print(mismatches)
            print(schema_dict)
            print("No Mismatch")


        # Save cleaned data
        df.write.mode("overwrite").parquet(output_path)
