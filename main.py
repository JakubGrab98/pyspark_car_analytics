import os

import streamlit as st
from minio import Minio
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from analytics.cars_filter import CarsFilter
from analytics.cars_reports import CarReport
from const import (
    MODEL_COLUMN,
    PRICE_COLUMN,
    MILEAGE_COLUMN,
)

load_dotenv()

def minio_upload_data(bucket_name, file_path):
    minio_client = Minio(
        os.getenv("NODE_IP"),  # Replace with any node's IP and port
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    minio_client.fput_object(bucket_name, file_path, file_path)
    print(f"Uploaded {file_path} to MinIO bucket {bucket_name}")


def read_data(spark: SparkSession, source_path: str="data/transform"):
    """
    Reads data from parquet files.
    :param spark: SparkSession
    :param source_path: Path to source folder.
    :return: DataFrame
    """
    df = spark.read.parquet(source_path)
    return df


if __name__ == "__main__":
    minio_upload_data("otomoto-scraper", "data/raw/otomoto_data.csv")
    # spark_session = (SparkSession.builder
    #         .appName("cars_analytics")
    #         .master("local")
    #         .getOrCreate()
    # )
    #
    # st.title("Cars Advertisement Analysis")
    #
    # with st.sidebar:
    #     producer = st.text_input("Enter producer name:")
    #     model = st.text_input("Enter model name:")
    #     min_year = st.number_input("Minimum production year:", value = 1900, min_value=1900, max_value=2100)
    #     max_year = st.number_input("Maximum production year:", value = 2999, min_value=1900, max_value=2999)
    #     min_price = st.number_input("Minimum price:", value=0, min_value=0)
    #     max_price = st.number_input("Maximum price:", value=0, min_value=0)
    #     if st.button("Restart"):
    #         st.empty()
    #
    # if st.button("Start Analysis"):
    #     if producer and model:
    #         base_df = read_data(spark_session).cache()
    #         with st.spinner("Analyzing data..."):
    #             car_filter = CarsFilter(producer, model, min_year, max_year, min_price, max_price)
    #             car_reports = CarReport(base_df, car_filter)
    #             st.markdown("## Car Analysis")
    #             st.markdown("### Model's statistics")
    #             st.table(car_reports.get_model_statistics())
    #             st.markdown("### Model With The Lowest Price")
    #             st.table(car_reports.lowest_value_by_column(PRICE_COLUMN))
    #             st.markdown("### Model With The Lowest Mileage")
    #             st.table(car_reports.lowest_value_by_column(MILEAGE_COLUMN))
    #             st.markdown("### Average Price By Other Models")
    #             st.bar_chart(
    #                 car_reports.compare_other_models(),
    #                 x=MODEL_COLUMN,
    #                 y="avg_price",
    #                 x_label="Model",
    #                 y_label="Average Price",
    #             )
    #             st.markdown("### All Advertises")
    #             st.table(car_reports.get_advertises().toPandas())
    #     else:
    #         st.error("Please fill in all fields.")
