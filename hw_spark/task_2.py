"""Find the most popular country where hotels are booked and searched from the same
country. Implement using scala or python. Create a separate application. Copy the
application to the archive. Make screenshots of results: before and after execution. """
import time

from pyspark.sql import SparkSession
from task_1 import build_hotel_dataframe


def most_popular_search_and_booked_country_with_agg(dataframe):
    """Find the most popular country where hotels are booked and searched from the same
    country with an aggregation.

    :param dataframe: A Spark Dataframe with hotels
    :type dataframe: Dataframe
    :return: None
    :rtype: NoneType
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
    filtered_df.filter(filtered_df["count"] == maximum).show()


def most_popular_search_and_booked_country_with_sort(dataframe):
    """Find the most popular country where hotels are booked and searched from the same
    country with a sort transformation.

    :param dataframe: A Spark Dataframe with hotels
    :type dataframe: Dataframe
    :return: None
    :rtype: NoneType
    """
    (
        dataframe.filter(
            dataframe["user_location_country"] == dataframe["hotel_country"]
        )
        .select("user_location_country", "hotel_country")
        .groupBy("user_location_country")
        .count()
        .sort("count", ascending=False)
        .show(1)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Task_2").getOrCreate()
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"

    # Try with sort
    start_time = time.time()
    most_popular_search_and_booked_country_with_sort(
        build_hotel_dataframe(spark, filepath)
    )
    print(f"--- Time with sort: {time.time() - start_time} seconds ---")

    # Try with agg
    start_time = time.time()
    most_popular_search_and_booked_country_with_agg(
        build_hotel_dataframe(spark, filepath)
    )
    print(f"--- Time with agg: {time.time() - start_time} seconds ---")
