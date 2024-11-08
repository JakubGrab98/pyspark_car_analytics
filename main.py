from pyspark.sql import SparkSession


spark = (SparkSession.builder
        .appName("cars_analytics")
        .master("local")
        .getOrCreate()
)

cars_df = (
    spark.read
    .option("header", "true")
    .csv("raw/vehicles.csv")
)

print(cars_df.show(truncate=False))
cars_df.printSchema()
print(cars_df.count())