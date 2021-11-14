"""Find top 3 hotels where people with children are interested but not booked in the
end. Implement using scala or python. Create a separate application. Copy the
application to the archive. Make screenshots of results: before and after execution. """
from pyspark.sql import DataFrame, SparkSession
from task_1 import build_hotel_dataframe


def top_3_hotels_with_children_not_booked(dataframe: DataFrame) -> DataFrame:
    """Find top 3 hotels where people with children are interested but not booked in the
    end.

        :param dataframe: A Spark Dataframe with hotels
        :type dataframe: Dataframe
        :return: DataFrame with hotels and count
        :rtype: DataFrame
    """
    return (
        dataframe.filter(
            (dataframe["srch_children_cnt"] > 0) & (dataframe["is_booking"] == 0)
        )
        .select("hotel_continent", "hotel_country", "hotel_market")
        .groupBy("hotel_continent", "hotel_country", "hotel_market")
        .count()
        .sort("count", ascending=False)
        .limit(3)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Task_3").getOrCreate()
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"
    top_3_hotels_with_children_not_booked(build_hotel_dataframe(spark, filepath)).show()
