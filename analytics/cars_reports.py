"""Module responsible for gathering reporting methods."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from analytics.cars_filter import CarsFilter
from const import *


class CarReport:
    def __init__(self, df: DataFrame, cars_filter: CarsFilter):
        """
        Initializes CarReport class.
        :param df: Car's dataframe.
        :param cars_filter: Instance of CarsFilter for filtering purposes.
        """
        self.df = df
        self.cars_filter = cars_filter

    def get_number_of_producer_advertises(self) -> int:
        """Retrieves number of producer's advertisement."""
        base_df = self.df.select(PRODUCER_COLUMN)
        result = self.cars_filter.filter_by_all_parameters(base_df).count()
        return result

    def get_model_statistics(self) -> DataFrame:
        """Retrieves car's model statistics"""
        statistics_df = (
            self.df.select(PRODUCER_COLUMN, MODEL_COLUMN, YEAR_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_all_parameters)
            .groupby(col(PRODUCER_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(PRODUCER_COLUMN)),
                    col(MODEL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MODEL_COLUMN)),
            )
            .agg(
                max(YEAR_COLUMN).alias("Max Year"),
                min(YEAR_COLUMN).alias("Min Year"),
                format_number(avg(PRICE_COLUMN), 0).alias("Avg Price"),
                format_number(max(PRICE_COLUMN), 0).alias("Max Price"),
                format_number(min(PRICE_COLUMN), 0).alias("Min Price"),
                format_number(count(PRICE_COLUMN), 0).alias("Advertisements Amount")
            )
        )
        return statistics_df

    def get_advertises(self) -> DataFrame:
        """
        Creates dataframe with details from advertisement for provided parameters.
        """
        advertises_df = (
            self.df
            .transform(self.cars_advertises_table)
            .transform(self.cars_filter.filter_by_all_parameters)
        )
        return advertises_df

    def compare_other_producers(self) -> DataFrame:
        """Creates comparison of producers prices based on year and price filter"""
        comparison_df = (
            self.df.select(PRODUCER_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_year)
            .transform(self.cars_filter.filter_by_price)
            .groupby(PRODUCER_COLUMN)
            .agg(
                format_number(avg(PRICE_COLUMN), 0).alias("avg_price"),
                format_number(max(PRICE_COLUMN), 0).alias("max_price"),
                format_number(min(PRICE_COLUMN), 0).alias("min_price"),
                format_number(count(PRICE_COLUMN), 0).alias("advertises_amount")
            )
            # .orderBy(col("avg_price").desc())
        )
        return comparison_df

    def compare_other_models(self) -> DataFrame:
        """Creates comparison of producers prices based on year and price filter"""
        comparison_df = (
            self.df.select(MODEL_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_year)
            .transform(self.cars_filter.filter_by_producer)
            .transform(self.cars_filter.filter_by_price)
            .groupby(MODEL_COLUMN)
            .agg(
                avg(PRICE_COLUMN).alias("avg_price"),
                max(PRICE_COLUMN).alias("max_price"),
                min(PRICE_COLUMN).alias("min_price"),
                count(PRICE_COLUMN).alias("advertises_amount")
            )
        )
        return comparison_df

    def lowest_value_by_column(self, column_name: str) -> DataFrame:
        """
        Finds the record with specific column the lowest value.
        :param column_name: Name of the column which will be analyzed.
        :return: DataFrame with car's advertisement details.
        """
        base_df = self.df.transform(self.cars_filter.filter_by_all_parameters)
        lowest_value = base_df.select(min(col(column_name))).collect()[0][0]
        lowest_value_df = (
            base_df.filter(col(column_name) == lowest_value)
            .transform(self.cars_advertises_table)
        )
        return lowest_value_df

    @staticmethod
    def cars_advertises_table(df: DataFrame) -> DataFrame:
        """
        Initial transformation of cars advertisement DataFrame.
        :param df: Car's DataFrame.
        :return: Transformed DataFrame with proper columns.
        """
        cars_df = df.select(
            col(PRODUCER_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(PRODUCER_COLUMN)),
            col(MODEL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MODEL_COLUMN)),
            col(VERSION_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(VERSION_COLUMN)),
            col(YEAR_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(YEAR_COLUMN)),
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
