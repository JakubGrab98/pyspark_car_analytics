from h11 import PRODUCT_ID
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from analytics.cars_filter import CarsFilter
from const import *


class CarReport:
    def __init__(self, df: DataFrame, cars_filter: CarsFilter):
        self.df = df
        self.cars_filter = cars_filter

    def get_number_of_producer_advertises(self) -> int:
        base_df = self.df.select(PRODUCER_COLUMN)
        result = self.cars_filter.filter_by_all_parameters(base_df).count()
        return result

    def get_model_statistics(self) -> DataFrame:
        statistics_df = (
            self.df.select(PRODUCER_COLUMN, MODEL_COLUMN, YEAR_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_all_parameters)
            .groupby(PRODUCER_COLUMN, MODEL_COLUMN)
            .agg(
                max(YEAR_COLUMN).alias("max_prod_year"),
                min(YEAR_COLUMN).alias("min_prod_year"),
                avg(PRICE_COLUMN).alias("avg_price"),
                max(PRICE_COLUMN).alias("max_price"),
                min(PRICE_COLUMN).alias("min_price"),
            )
        )
        return statistics_df

    def get_advertises(self) -> DataFrame:
        advertises_df = (
            self.df
            .transform(self.cars_advertises_table)
            .transform(self.cars_filter.filter_by_all_parameters)
        )
        return advertises_df

    def compare_other_producers(self) -> DataFrame:
        comparison_df = (
            self.df.select(PRODUCER_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_producer)
            .transform(self.cars_filter.filter_by_year)
            .transform(self.cars_filter.filter_by_price)
            .groupby(PRODUCER_COLUMN)
            .agg(
                avg(PRICE_COLUMN).alias("avg_price"),
                max(PRICE_COLUMN).alias("max_price"),
                min(PRICE_COLUMN).alias("min_price"),
                count(PRICE_COLUMN).alias("advertises_amount")
            )
        )
        return comparison_df

    def lowest_value_by_column(self, column_name: str) -> DataFrame:
        base_df = self.df.transform(self.cars_filter.filter_by_all_parameters)
        lowest_value = base_df.select(column_name).min()
        lowest_value_df = (
            base_df.filter(column_name == lowest_value)
            .transform(self.cars_advertises_table)
        )
        return lowest_value_df

    @staticmethod
    def cars_advertises_table(df: DataFrame) -> DataFrame:
        cars_df = df.select(
            col(PRODUCER_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(PRODUCER_COLUMN)),
            col(MODEL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MODEL_COLUMN)),
            col(VERSION_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(VERSION_COLUMN)),
            concat(
                col(MILEAGE_COLUMN),
                lit(" "),
                col(MILEAGE_UNIT_COLUMN),
            ).alias(REPORT_COLUMNS_MAPPING.get(MILEAGE_COLUMN)),
            concat(
                format_number(col(PRICE_COLUMN), 0),
                lit(" PLN"),
            ).alias(REPORT_COLUMNS_MAPPING.get(PRICE_COLUMN)),
            col(URL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(URL_COLUMN)),
        )
        return cars_df
