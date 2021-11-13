"""Find the most popular country where hotels are booked and searched from the same
country. Implement using scala or python. Create a separate application. Copy the
application to the archive. Make screenshots of results: before and after execution. """
import time

from pyspark.sql import DataFrame, SparkSession
from task_1 import build_hotel_dataframe


def most_popular_search_and_booked_country_with_agg(dataframe: DataFrame) -> DataFrame:
    """Find the most popular country where hotels are booked and searched from the same
    country with an aggregation.

    :param dataframe: A Spark Dataframe with hotels
    :type dataframe: Dataframe
    :return: DataFrame with country and count
    :rtype: DataFrame
    """
    filtered_df = (
        dataframe.filter(
            dataframe["user_location_country"] == dataframe["hotel_country"]
        )
        .select("user_location_country", "hotel_country")
        .groupBy("user_location_country")
        .count()
    )
    maximum = filtered_df.agg({"count": "max"}).collect()[0]["max(count)"]
    return filtered_df.filter(filtered_df["count"] == maximum)


def most_popular_search_and_booked_country_with_sort(dataframe: DataFrame) -> DataFrame:
    """Find the most popular country where hotels are booked and searched from the same
    country with a sort transformation.

    :param dataframe: A Spark Dataframe with hotels
    :type dataframe: Dataframe
    :return: DataFrame with country and count
    :rtype: DataFrame
    """
    return (
        dataframe.filter(
            dataframe["user_location_country"] == dataframe["hotel_country"]
        )
        .select("user_location_country", "hotel_country")
        .groupBy("user_location_country")
        .count()
        .sort("count", ascending=False)
        .limit(1)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Task_2").getOrCreate()
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"

    # Try with sort
    start_time = time.time()
    most_popular_search_and_booked_country_with_sort(
        build_hotel_dataframe(spark, filepath)
    ).show()
    print(f"--- Time with sort: {time.time() - start_time} seconds ---")

    # Try with agg
    start_time = time.time()
    most_popular_search_and_booked_country_with_agg(
        build_hotel_dataframe(spark, filepath)
    ).show()
    print(f"--- Time with agg: {time.time() - start_time} seconds ---")
