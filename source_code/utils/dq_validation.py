from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

from source_code.utils.date_parse import parse_multiple_date_formats
from source_code.utils.logger import get_logger

logger = get_logger(__name__)


def get_columns_for_check(df: DataFrame, columns_config) -> list:
    """
    Return either all DataFrame columns or the subset specified in config.
    """
    return df.columns if columns_config == "all" else columns_config


def validate_schema(df: DataFrame, expected_schema: dict) -> list:
    """
    Compare the actual DataFrame schema with an expected schema.
    """
    actual_schema = {
        field.name: field.dataType.simpleString() for field in df.schema.fields
    }
    mismatches = [
        (
            f"{c} ("
            f"expected: {expected_schema[c]}, "
            f"actual: {actual_schema.get(c, 'MISSING')})"
        )
        for c in expected_schema
        if expected_schema[c] != actual_schema.get(c)
    ]
    if mismatches:
        logger.warning("Schema mismatches detected: %s", mismatches)
    else:
        logger.info("Schema validation passed.")
    return mismatches


def remove_nulls(df: DataFrame, columns: list, output_path: str) -> DataFrame:
    """
    Replace empty strings with nulls, log null rows, and drop them from DataFrame.
    """
    logger.info("Replacing empty strings with nulls for columns: %s", columns)
    df_na = df.select(
        [
            (
                when(trim(col(c)) == "", None).otherwise(trim(col(c))).alias(c)
                if c in columns
                else col(c)
            )
            for c in df.columns
        ]
    )
    null_rows = df_na.filter(
        " OR ".join([f"{col_name} IS NULL" for col_name in columns])
    )

    if not null_rows.isEmpty():
        logger.info("Null rows detected. Writing to: %s", output_path)
        null_rows.write.mode("overwrite").csv(output_path, header=True)
    else:
        logger.info("No null rows found in specified columns.")

    cleaned_df = df_na.na.drop(subset=columns)
    logger.info("Null values removed. Remaining rows: %d", cleaned_df.count())
    return cleaned_df


def remove_duplicates(df: DataFrame, columns: list, output_path: str) -> DataFrame:
    """
    Detect and write duplicate rows to disk, then remove them from DataFrame.
    """
    logger.info("Checking for duplicates based on columns: %s", columns)
    windowed = df.groupBy(columns).count().filter("count > 1")
    duplicate_rows = df.join(windowed, on=columns, how="inner")

    if duplicate_rows.count() > 0:
        logger.warning("Duplicate rows found. Writing to: %s", output_path)
        duplicate_rows.write.mode("overwrite").csv(output_path, header=True)
    else:
        logger.info("No duplicate rows found.")

    deduplicated_df = df.dropDuplicates(subset=columns)
    logger.info("Duplicates removed. Remaining rows: %d", deduplicated_df.count())
    return deduplicated_df


def enforce_schema(df: DataFrame, schema: dict) -> DataFrame:
    """
    Enforce a schema on an existing DataFrame with type-specific logic.
    """
    logger.info("Enforcing schema: %s", schema)

    def get_data_type(data_type_str):
        if isinstance(data_type_str, str):
            type_str = data_type_str.lower()
            if type_str == "date":
                return DateType()
            elif type_str == "string":
                return StringType()
            elif type_str == "int":
                return IntegerType()
            elif type_str == "double":
                return DoubleType()
            elif type_str == "float":
                return FloatType()
            elif type_str == "long":
                return LongType()
            elif type_str == "boolean":
                return BooleanType()
            else:
                logger.error("Unsupported data type string: %s", data_type_str)
                raise ValueError(f"Unsupported data type string: {data_type_str}")
        return data_type_str

    for col_name, data_type in schema.items():
        if col_name not in df.columns:
            logger.warning(
                "Column '%s' not found in DataFrame. Skipping schema enforcement.",
                col_name,
            )
            continue

        data_type = get_data_type(data_type)
        actual_type = dict(df.dtypes).get(col_name)

        if isinstance(data_type, DateType):
            logger.info("Parsing dates for column '%s'", col_name)
            df = parse_multiple_date_formats(df, col_name, col_name)
            df = df.fillna({col_name: "1900-01-01"})
        else:
            if actual_type != data_type.simpleString():
                logger.info(
                    "Casting column '%s' from %s to %s",
                    col_name,
                    actual_type,
                    data_type.simpleString(),
                )
                df = df.withColumn(col_name, col(col_name).cast(data_type))
            else:
                logger.info(
                    "Column '%s' is already of type %s. Skipping cast.",
                    col_name,
                    data_type.simpleString(),
                )

    logger.info("Schema enforcement completed.")
    return df
