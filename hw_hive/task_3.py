"""Find top 3 hotels where people with children are interested but not booked in the
end. Implement using scala or python. Create a separate application. Copy the
application to the archive. Make screenshots of results: before and after execution. """
from pyspark.sql import SparkSession
from task_1 import build_hotel_dataframe


def top_3_hotels_with_children_not_booked(dataframe):
    pass


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySpark_Tutorial")
        .getOrCreate()
    )
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"
    top_3_hotels_with_children_not_booked(build_hotel_dataframe(spark, filepath))
