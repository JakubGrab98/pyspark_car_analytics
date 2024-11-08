from pyspark.sql import SparkSession


class CarsTransformation:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_data(self, csv_file: str):
        df = (
            spark.read
            .option("header", "true")
            .csv(csv_file)
        )
        return df