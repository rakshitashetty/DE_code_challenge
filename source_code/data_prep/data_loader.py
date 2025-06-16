from pyspark.sql import SparkSession

from source_code.utils.dq_validation import (
    enforce_schema,
    get_columns_for_check,
    remove_duplicates,
    remove_nulls,
    validate_schema,
)
from source_code.utils.logger import get_logger

logger = get_logger(__name__)


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
        logger.info("Initialized GenericETLJob with configuration: %s", config)

    def load(self):
        """
        Executes the full data loading process:
        - Reads raw data
        - Applies null checks and duplicate removal
        - Enforces schema
        - Saves the clean output
        """
        input_path = self.config["input_path"]
        output_path = self.config["output_path"]
        schema_dict = self.config["schema"]

        logger.info("Starting the ETL job: loading data from %s", input_path)
        try:
            # Step 1: Read raw data
            logger.info("Reading raw data from input path: %s", input_path)
            df = self.spark.read.option("header", True).csv(input_path)

            logger.info(
                "Successfully loaded data with %d rows and %d columns.",
                df.count(),
                len(df.columns),
            )

            # Step 2: Clean nulls
            logger.info("Cleaning null values")
            columns_for_null_check = get_columns_for_check(
                df, self.config["null_check"]["columns"]
            )
            df = remove_nulls(
                df, columns_for_null_check, self.config["null_check"]["null_path"]
            )
            logger.info(
                "Successfully removed null values. Cleaned data has %d rows.",
                df.count(),
            )

            # Step 3: Deduplicate
            logger.info(
                "Removing duplicates based on columns: %s",
                self.config["dup_check"]["columns"],
            )
            columns_for_dup_check = get_columns_for_check(
                df, self.config["dup_check"]["columns"]
            )
            df = remove_duplicates(
                df, columns_for_dup_check, self.config["dup_check"]["dup_path"]
            )
            logger.info(
                "Successfully removed duplicates. Cleaned data has %d rows.", df.count()
            )

            # Step 4: Schema enforcement
            logger.info("Enforcing schema on the DataFrame.")
            df = enforce_schema(df, schema_dict)
            logger.info("Schema enforcement completed.")

            # Step 5: Schema validation
            logger.info("Validating schema...")
            mismatches = validate_schema(df, schema_dict)
            if mismatches:
                logger.error("Schema mismatches found: %s", mismatches)
                logger.debug("Expected schema: %s", schema_dict)
            else:
                logger.info("No schema mismatches found.")
                logger.debug("Expected schema: %s", schema_dict)

            # Step 6: Save cleaned data
            logger.info("Saving the cleaned data to output path: %s", output_path)
            df.write.mode("overwrite").parquet(output_path)
            logger.info("Successfully saved cleaned data to: %s", output_path)

        except Exception as e:
            logger.error("ETL job failed due to error: %s", str(e))
            raise  # Reraise the exception after logging it
