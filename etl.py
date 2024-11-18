"""Module combines ETL process."""
import asyncio
from pyspark.sql import SparkSession
from transform.transform import clean_data, get_price_in_pln, save_data_to_parquet
from scraper.otomoto_scraper import AdvertisementScraper
from rates.nbp_rates import Rates
from const import NBP_API
from logging_configs.log_setup import setup_logging


def transformation(spark: SparkSession):
    """
    Gathering transformation method for ETL.
    :param spark: SparkSession
    """
    rates = Rates(NBP_API, spark)
    rates_df = rates.get_rates("a")

    raw_cars_df = (
        spark.read
        .option("header", "true")
        .csv("data/raw/otomoto_data.csv")
    )

    cleaned_car_df = raw_cars_df.transform(clean_data)
    updated_price_df = get_price_in_pln(cleaned_car_df, rates_df)
    updated_price_df.show()
    updated_price_df.printSchema()
    save_data_to_parquet(updated_price_df, ["year", "producer"], "data/transform")


BASE_URL = "https://www.otomoto.pl/osobowe?page="
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

if __name__ == "__main__":
    setup_logging()

    scraper = AdvertisementScraper(BASE_URL, headers)
    asyncio.run(scraper.scraper())
    scraper.save_data_to_csv()

    spark_session = SparkSession.builder.appName("Otomoto").master("local").getOrCreate()
    transformation(spark_session)
