from pyspark.sql import SparkSession
from transform.transform import clean_data, get_price_in_pln, save_data_to_parquet
from rates.nbp_rates import Rates
from const import NBP_API


def transformation(spark):
    rates = Rates(NBP_API, spark)
    rates_df = rates.get_rates("a")

    raw_cars_df = (
        spark.read
        .option("header", "true")
        .csv("data/raw/cars_data.csv")
    )

    cleaned_car_df = raw_cars_df.transform(clean_data)
    updated_price_df = get_price_in_pln(cleaned_car_df, rates_df)
    save_data_to_parquet(updated_price_df, ["year", "producer"], "data/transform")

def main():
    spark = (SparkSession.builder
            .appName("cars_analytics")
            .master("local")
            .getOrCreate()
    )
    transformation(spark)


if __name__ == "__main__":
    main()