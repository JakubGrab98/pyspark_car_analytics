from pyspark.sql import SparkSession, DataFrame


class CarsAnalytics:

    TRANSFORMED_DATA = "data/transform"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_transformed_data(self) -> DataFrame:
        df = (
            self.spark_session.read
            .parquet(self.TRANSFORMED_DATA)
        )
        return df

