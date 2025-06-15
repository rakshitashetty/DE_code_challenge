from src.data_prep.base_etl_job import BaseETLJob


class StoresETLJob(BaseETLJob):
    def run(self):
        df = self.spark.read.csv(self.config["file_path"], header=True, inferSchema=None)

        columns_for_null_check = self.get_columns_for_check(self, df, self.config['null_check']['columns'])
        df = self.remove_nulls(self, df, columns_for_null_check, self.config['null_check']['null_path'])

        columns_for_dup_check = self.get_columns_for_check(self, df, self.config['dup_check']['columns'])
        df = self.remove_duplicates(self, df, columns_for_dup_check, self.config['dup_check']['dup_path'])

        df=self.enforce_schema(self,df,self.config["schema"].items())

        schema_mismatches = self.validate_schema(self, df, self.config["schema"])

        if schema_mismatches:
            print("Schema mismatches:", schema_mismatches)
        else:
            print("No Schema Mismatch")

        df.write.parquet(self.config["clean_path"], mode="overwrite")
