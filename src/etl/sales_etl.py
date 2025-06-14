from src.etl.base_etl_job import BaseETLJob
from src.utils.date_utils import parse_multiple_date_formats

class SalesETLJob(BaseETLJob):
    def run(self):
        df = self.spark.read.csv(self.config["file_path"], header=True, inferSchema=True)
        df = parse_multiple_date_formats(df, "transaction_date", "transaction_date")
        df.write.parquet(self.config["output_path"], mode="overwrite")
