from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class CarsTransformation:

    COLUMNS_LIST = [
        "id", "url", "region", "region_url", "price",
        "year", "manufacturer", "model", "condition",
        "fuel", "odometer", "transmission", "VIN",
        "drive", "size", "type", "paint_color",
        "description", "state", "lat", "long", "posting_date",
    ]

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_data(self, csv_file: str):
        df = (
            self.spark_session.read
            .option("header", "true")
            .csv(csv_file)
        )
        return df

    def clean_data(self, df: DataFrame) -> DataFrame:
        clean_df = (
            df
            .withColumn("price", col("price").cast(IntegerType()))
            .withColumn("odometer", col("odometer").cast(LongType()))
            .withColumn("lat", col("lat").cast(FloatType()))
            .withColumn("long", col("long").cast(FloatType()))
            .withColumn("posting_date", substring(col("posting_date"), 1, 10).cast(DateType()))
            .withColumn("posting_year", year(col("posting_date")))
            .withColumn("posting_month", month(col("posting_date")))
            .withColumn("currency", lit("USD"))
            .filter(
                (col("model").isNotNull())
                & (col("manufacturer").isNotNull())
                & (col("price") > 0)
            )
            .select(self.COLUMNS_LIST)
        )
        return clean_df