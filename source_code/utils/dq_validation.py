from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, col, when
from pyspark.sql.types import DateType
from source_code.utils.date_parse import parse_multiple_date_formats

def get_columns_for_check(df: DataFrame, columns_config) -> list:
    """
    Return either all DataFrame columns or the subset specified in config.

    Parameters:
        df (DataFrame): Input DataFrame.
        columns_config (list or str): Column names to check, or "all".

    Returns:
        list: List of column names to process.
    """
    return df.columns if columns_config == "all" else columns_config


def validate_schema(df: DataFrame, expected_schema: dict) -> list:
    """
    Compare the actual DataFrame schema with an expected schema.

    Parameters:
        df (DataFrame): DataFrame whose schema is to be validated.
        expected_schema (dict): Dictionary of expected schema {column: datatype_str}.

    Returns:
        list: List of mismatched column messages, or empty if no issues.
    """
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    mismatches = [
        f"{c} (expected: {expected_schema[c]}, actual: {actual_schema.get(col, 'MISSING')})"
        for c in expected_schema
        if expected_schema[c] != actual_schema.get(c)
    ]
    return mismatches


def remove_nulls(df: DataFrame, columns: list, output_path: str) -> DataFrame:
    """
    Replace empty strings with nulls, log null rows, and drop them from DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.
        columns (list): List of columns to check for nulls or blanks.
        output_path (str): Path to write the rows with nulls.

    Returns:
        DataFrame: Cleaned DataFrame with nulls removed.
    """
    df_na = df.select([
        when(trim(col(c)) == '', None).otherwise(trim(col(c))).alias(c) if c in columns else col(c)
        for c in df.columns
    ])
    null_rows = df_na.filter(" OR ".join([f"{col_name} IS NULL" for col_name in columns]))
    if not null_rows.isEmpty():
        null_rows.write.mode("overwrite").csv(output_path, header=True)
    return df.na.drop(subset=columns)


def remove_duplicates(df: DataFrame, columns: list, output_path: str) -> DataFrame:
    """
    Detect and write duplicate rows to disk, then remove them from DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.
        columns (list): List of columns used to identify duplicates.
        output_path (str): Path to write the duplicate rows.

    Returns:
        DataFrame: Deduplicated DataFrame.
    """
    windowed = df.groupBy(columns).count().filter("count > 1")
    duplicate_rows = df.join(windowed, on=columns, how='inner')
    if duplicate_rows.count() > 0:
        duplicate_rows.write.mode("overwrite").csv(output_path, header=True)
    return df.dropDuplicates(subset=columns)


from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StringType, IntegerType

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StringType, IntegerType, DoubleType, FloatType, LongType, BooleanType


def enforce_schema(df: DataFrame, schema: dict) -> DataFrame:
    """
    Enforce a schema on an existing DataFrame with type-specific logic.

    Parameters:
        df (DataFrame): The input DataFrame.
        schema (dict): Dictionary of column names and target data types (as PySpark DataType objects or strings).

    Returns:
        DataFrame: DataFrame with schema enforced, including date parsing.
    """

    # Helper function to convert strings to PySpark DataType objects
    def get_data_type(data_type_str):
        if isinstance(data_type_str, str):
            if data_type_str.lower() == "date":
                return DateType()
            elif data_type_str.lower() == "string":
                return StringType()
            elif data_type_str.lower() == "int":
                return IntegerType()
            elif data_type_str.lower() == "double":
                return DoubleType()
            elif data_type_str.lower() == "float":
                return FloatType()
            elif data_type_str.lower() == "long":
                return LongType()
            elif data_type_str.lower() == "boolean":
                return BooleanType()
            # Add more types as needed
            else:
                raise ValueError(f"Unsupported data type string: {data_type_str}")
        return data_type_str

    for col_name, data_type in schema.items():
        if col_name not in df.columns:
            # Log or raise a warning if the column doesn't exist in the DataFrame
            print(f"Warning: Column '{col_name}' not found in DataFrame. Skipping schema enforcement.")
            continue

        # Convert data_type to PySpark DataType if it's a string
        data_type = get_data_type(data_type)

        # Get the actual column type
        actual_type = dict(df.dtypes).get(col_name)

        if isinstance(data_type, DateType):
            # Handle date columns with multiple date formats
            df = parse_multiple_date_formats(df, col_name, col_name)
            # Optionally handle parsing failures here
            df = df.fillna({col_name: "1900-01-01"})  # Use a default date if parsing fails
        else:
            # Only cast if the column type is different from the expected type
            if actual_type != data_type.simpleString():
                df = df.withColumn(col_name, col(col_name).cast(data_type))
            else:
                print(f"Column '{col_name}' is already of type {data_type.simpleString()}. Skipping.")

    return df
