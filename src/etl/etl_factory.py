from src.etl.sales_etl import SalesETLJob
# from src.etl.product_etl import ProductETLJob
# from src.etl.store_etl import StoreETLJob

def get_etl_job(file_type, spark, config):
    if file_type == "sales":
        return SalesETLJob(spark, config)
    elif file_type == "products":
        return ProductETLJob(spark, config)
    elif file_type == "stores":
        return StoreETLJob(spark, config)
    else:
        raise ValueError(f"Unknown file type: {file_type}")
