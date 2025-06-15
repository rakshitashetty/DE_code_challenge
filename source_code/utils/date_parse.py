from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, when, expr

def parse_multiple_date_formats(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Efficiently parses dates from multiple known formats, with disambiguation for MM/dd/yyyy and MM-dd-yyyy.
    """

    parsed_date = when(col(input_col).rlike(r"^\d{4}-\d{2}-\d{2}$"),
                       to_date(col(input_col), "yyyy-MM-dd")) \
        .when(col(input_col).rlike(r"^\d{2}-\d{2}-\d{4}$") & (expr(f"int(split({input_col}, '-')[0]) > 12")),
              to_date(col(input_col), "dd-MM-yyyy")) \
        .when(col(input_col).rlike(r"^\d{2}-\d{2}-\d{4}$") & (expr(f"int(split({input_col}, '-')[0]) <= 12")),
              to_date(col(input_col), "MM-dd-yyyy")) \
        .when(col(input_col).rlike(r"^\d{2}/\d{2}/\d{4}$") & (expr(f"int(split({input_col}, '/')[0]) > 12")),
              to_date(col(input_col), "dd/MM/yyyy")) \
        .when(col(input_col).rlike(r"^\d{2}/\d{2}/\d{4}$") & (expr(f"int(split({input_col}, '/')[0]) <= 12")),
              to_date(col(input_col), "MM/dd/yyyy")) \
        .when(col(input_col).rlike(r"^\d{4}/\d{2}/\d{2}$"),
              to_date(col(input_col), "yyyy/MM/dd")) \
        .when(col(input_col).rlike(r"^\w+ \d{2}, \d{4}$"),
              to_date(col(input_col), "MMMM dd, yyyy")) \
        .otherwise(None)

    return df.withColumn(output_col, parsed_date)
