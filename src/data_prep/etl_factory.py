from src.data_prep.sales_etl import SalesETLJob
from src.data_prep.products_etl import ProductsETLJob
from src.data_prep.stores_etl import StoresETLJob

def get_etl_job(file_type, spark, config):
    if file_type == "sales":
        print("Sales ETL job starting")
        return SalesETLJob(spark, config)
    elif file_type == "products":
        print("Products ETL job starting")
        return ProductsETLJob(spark, config)
    elif file_type == "stores":
        print("Stores ETL job starting")
        return StoresETLJob(spark, config)
    else:
        raise ValueError(f"Unknown file type: {file_type}")
