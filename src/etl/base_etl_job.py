from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class BaseETLJob(ABC):
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    @abstractmethod
    def run(self):
        pass
