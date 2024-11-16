import streamlit as st
from pygments.lexer import default
from pyspark.sql import SparkSession
from transform.transform import clean_data, get_price_in_pln, save_data_to_parquet
from analytics.cars_filter import CarsFilter
from analytics.cars_reports import CarReport
from rates.nbp_rates import Rates
from const import NBP_API


def transformation(spark):
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

def reporting(spark: SparkSession):
    cleaned_df = spark.read.parquet("data/transform")
    car_filter = CarsFilter("Audi", "A4")
    car_reports = CarReport(cleaned_df, car_filter)
    car_reports.get_model_statistics().show()
    print(car_reports.get_number_of_producer_advertises())


def main():
    spark = (SparkSession.builder
            .appName("cars_analytics")
            .master("local")
            .getOrCreate()
    )
    transformation(spark)
    transformed_df = spark.read.parquet("data/transform")
    return transformed_df


if __name__ == "__main__":
    base_df = main().cache()
    st.title("Cars Advertisement Analysis")

    with st.sidebar:
        producer = st.text_input("Enter producer name:")
        model = st.text_input("Enter model name:")
        min_year = st.number_input("Minimum production year:", value = 1900, min_value=1900, max_value=2100)
        max_year = st.number_input("Maximum production year:", value = 2999, min_value=1900, max_value=2999)
        min_price = st.number_input("Minimum price:", value=0, min_value=0)
        max_price = st.number_input("Maximum price:", value=0, min_value=0)


    if st.button("Start Analysis"):
        if producer and model:
            with st.spinner("Analyzing data..."):
                car_filter = CarsFilter(producer, model, min_year, max_year, min_price, max_price)
                car_reports = CarReport(base_df, car_filter)
                st.markdown("## Car Analysis")
                st.table(car_reports.get_model_statistics())
                st.table(car_reports.get_advertises())
                st.table(car_reports.compare_other_producers())
        else:
            st.error("Please fill in all fields.")