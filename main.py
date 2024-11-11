from pyspark.sql import SparkSession
from transform.transform import CarsTransformation
from rates.nbp_rates import Rates
from analytics.cars_analytics import CarsAnalytics


NBP_API = r"https://api.nbp.pl/api/exchangerates/tables"

def main():
    spark = (SparkSession.builder
            .appName("cars_analytics")
            .master("local")
            .getOrCreate()
    )

    rates = Rates(NBP_API, spark)
    transform = CarsTransformation(spark)

    raw_df = transform.load_data("data/raw/vehicles.csv")
    cleaned_df = transform.clean_data(raw_df)
    rates_df = rates.get_rates("a")
    df_price_pln = transform.get_price_in_pln(cleaned_df, rates_df)
    transform.save_data_to_parquet(df_price_pln, "data/transform")

    analytics = CarsAnalytics(spark)
    analytics.avg_manufacturer_price_by_year("bmw").show()
    analytics.get_model_statistics("bmw", "5 series").show()

if __name__ == "__main__":
    main()
