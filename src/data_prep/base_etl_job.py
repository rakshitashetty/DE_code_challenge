from abc import ABC, abstractmethod
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from src.utils.date_utils import parse_multiple_date_formats
from pyspark.sql.types import DateType, StructType


class BaseETLJob(ABC):
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    @staticmethod
    def get_columns_for_check(self,df, columns_config):
        """
        Returns all columns if 'all' is specified, else returns selected ones.
        """
        if columns_config == "all":
            return df.columns
        return columns_config

    @staticmethod
    def validate_schema(self,df: DataFrame, expected_schema: dict):
        actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        mismatches = [
            f"{i} (expected: {expected_schema[i]}, actual: {actual_schema.get(i, 'MISSING')})"
            for i in expected_schema
            if expected_schema[i] != actual_schema.get(i)
        ]
        return mismatches


    @staticmethod
    def remove_nulls(self,df: DataFrame, columns: list, output_path: str) -> DataFrame:
        df = df.select([
            when(trim(col(c)) == '', None).otherwise(trim(col(c))).alias(c) if c in columns else col(c)
            for c in df.columns
        ])
        null_rows = df.filter(" OR ".join([f"{col_name} IS NULL" for col_name in columns]))
        null_rows.write.mode("overwrite").csv(output_path, header=True)
        #self.logger.info(f"Null rows saved to {output_path}")
        return df.dropna(subset=columns)

    @staticmethod
    def remove_duplicates(self, df: DataFrame, columns: list, output_path: str) -> DataFrame:
        windowed = df.groupBy(columns).count().filter("count > 1")
        duplicate_rows = df.join(windowed, on=columns, how='inner')
        duplicate_rows.write.mode("overwrite").csv(output_path, header=True)
        #self.logger.info(f"Duplicate rows saved to {output_path}")
        return df.dropDuplicates(subset=columns)

    @staticmethod
    def enforce_schema(self,df: DataFrame, schema: dict) -> DataFrame:
        """
        Enforce a schema on an existing DataFrame with type-specific logic.

        Parameters:
            df (DataFrame): The input DataFrame.
            schema (StructType): Target schema to enforce.
        Returns:
            DataFrame: DataFrame with schema enforced.
        """

        for col_name,data_type in schema:

            if col_name not in df.columns:
                continue  # Skip columns not in DataFrame

            if isinstance(data_type, DateType):
                # Custom date parsing logic (e.g., multiple formats)
                df = parse_multiple_date_formats(df, col_name, col_name)
            else:
                df = df.withColumn(col_name, col(col_name).cast(data_type))

        return df

    @abstractmethod
    def run(self):
        pass