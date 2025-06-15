from src.data_prep.base_load_job import BaseETLJob


class GenericETLJob(BaseETLJob):
    def load(self):
        input_path = self.config["input_path"]
        output_path = self.config["output_path"]
        schema_dict = self.config["schema"]

        # Read data
        df = self.spark.read.option("header", True).csv(input_path)


        # Clean nulls
        columns_for_null_check = self.get_columns_for_check(df, self.config['null_check']['columns'])
        df = self.remove_nulls(df, columns_for_null_check, self.config['null_check']['null_path'])

        # Deduplicate
        columns_for_dup_check = self.get_columns_for_check(df, self.config['dup_check']['columns'])
        df = self.remove_duplicates(df, columns_for_dup_check, self.config['dup_check']['dup_path'])

        # Schema enforcement
        df = self.enforce_schema(df, schema_dict)

        # Save cleaned data
        df.write.mode("overwrite").parquet(output_path)
