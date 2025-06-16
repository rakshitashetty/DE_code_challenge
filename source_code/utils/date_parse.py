from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, to_date, when

from source_code.utils.logger import get_logger

# Get the logger
logger = get_logger(__name__)


def parse_multiple_date_formats(
    df: DataFrame, input_col: str, output_col: str
) -> DataFrame:
    """
    Parses dates from multiple known string formats into a standard format.

    This function handles a variety of common date formats such as:
    - 'yyyy-MM-dd'
    - 'MM/dd/yyyy'
    - 'MM-dd-yyyy'
    - 'dd-MM-yyyy'
    & more

    Args:
        df (DataFrame): Input Spark DataFrame containing the date column.
        input_col (str): Name of the column with raw date strings.
        output_col (str): Name of the new column to store parsed dates.

    Returns:
        df: DataFrame with an additional column containing normalized date values.
    """

    logger.info(
        "Starting date parsing for column '%s'. Total rows: %d", input_col, df.count()
    )

    parsed_date = (
        when(
            col(input_col).rlike(r"^\d{4}-\d{2}-\d{2}$"),
            to_date(col(input_col), "yyyy-MM-dd"),
        )
        .when(
            col(input_col).rlike(r"^\d{2}-\d{2}-\d{4}$")
            & (expr(f"int(split({input_col}, '-')[0]) > 12")),
            to_date(col(input_col), "dd-MM-yyyy"),
        )
        .when(
            col(input_col).rlike(r"^\d{2}-\d{2}-\d{4}$")
            & (expr(f"int(split({input_col}, '-')[0]) <= 12")),
            to_date(col(input_col), "MM-dd-yyyy"),
        )
        .when(
            col(input_col).rlike(r"^\d{2}/\d{2}/\d{4}$")
            & (expr(f"int(split({input_col}, '/')[0]) > 12")),
            to_date(col(input_col), "dd/MM/yyyy"),
        )
        .when(
            col(input_col).rlike(r"^\d{2}/\d{2}/\d{4}$")
            & (expr(f"int(split({input_col}, '/')[0]) <= 12")),
            to_date(col(input_col), "MM/dd/yyyy"),
        )
        .when(
            col(input_col).rlike(r"^\d{4}/\d{2}/\d{2}$"),
            to_date(col(input_col), "yyyy/MM/dd"),
        )
        .when(
            col(input_col).rlike(r"^\w+ \d{2}, \d{4}$"),
            to_date(col(input_col), "MMMM dd, yyyy"),
        )
        .otherwise(None)
    )

    # Log which formats are being used for parsing
    formats_used = [
        "yyyy-MM-dd",
        "dd-MM-yyyy",
        "MM-dd-yyyy",
        "dd/MM/yyyy",
        "MM/dd/yyyy",
        "yyyy/MM/dd",
        "MMMM dd, yyyy",
    ]
    logger.info("Attempting to parse using formats: %s", ", ".join(formats_used))

    # Execute transformation
    df_with_parsed_dates = df.withColumn(output_col, parsed_date)

    # Log completion
    logger.info("Date parsing complete for column '%s'.", input_col)

    # Check for any null values after parsing
    null_count = df_with_parsed_dates.filter(col(output_col).isNull()).count()
    if null_count > 0:
        logger.warning("Found %d rows with null values after date parsing.", null_count)
    else:
        logger.info("All rows successfully parsed. No nulls found in '%s'.", output_col)

    return df_with_parsed_dates
