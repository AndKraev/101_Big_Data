"""Find the most popular country where hotels are booked and searched from the same
country. Implement using scala or python. Create a separate application. Copy the
application to the archive. Make screenshots of results: before and after execution. """
from pyspark.sql import SparkSession
from task_1 import build_hotel_dataframe


def most_popular_search_and_booked_country(dataframe):
    (
        dataframe.filter(
            dataframe["user_location_country"] == dataframe["hotel_country"]
        )
        .select("user_location_country")
        .groupBy("user_location_country")
        .count()
        .agg({"count": "max"})
        .show()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySpark_Tutorial")
        .getOrCreate()
    )
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"
    most_popular_search_and_booked_country(build_hotel_dataframe(spark, filepath))
