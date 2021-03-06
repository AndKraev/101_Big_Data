"""Find top 3 most popular hotels between couples. (treat hotel as composite key of
continent, country and market). Implement using scala or python. Create a separate
application. Copy the application to the archive. Make screenshots of results: before
and after execution."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ByteType,
    DateType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)


def build_hotel_dataframe(sparksession: SparkSession, filepath: str) -> DataFrame:
    """Creates a dataframe from train.csv

    :param sparksession: The entry point into all functionality in Spark is the
    SparkSession class.
    :type sparksession: SparkSession
    :param filepath: Path to train.csv
    :type filepath: String
    :return: Spark Dataframe with hotels
    :rtype: Dataframe
    """
    schema = StructType(
        [
            StructField("date_time", TimestampType()),
            StructField("site_name", IntegerType()),
            StructField("posa_continent", ByteType()),
            StructField("user_location_country", IntegerType()),
            StructField("user_location_region", IntegerType()),
            StructField("user_location_city", IntegerType()),
            StructField("orig_destination_distance", DoubleType()),
            StructField("user_id", IntegerType()),
            StructField("is_mobile", ByteType()),
            StructField("is_package", ByteType()),
            StructField("channel", IntegerType()),
            StructField("srch_ci", DateType()),
            StructField("srch_co", DateType()),
            StructField("srch_adults_cnt", ByteType()),
            StructField("srch_children_cnt", ByteType()),
            StructField("srch_rm_cnt", IntegerType()),
            StructField("srch_destination_id", IntegerType()),
            StructField("srch_destination_type_id", IntegerType()),
            StructField("is_booking", ByteType()),
            StructField("cnt", IntegerType()),
            StructField("hotel_continent", ByteType()),
            StructField("hotel_country", IntegerType()),
            StructField("hotel_market", IntegerType()),
            StructField("hotel_cluster", IntegerType()),
        ]
    )
    return sparksession.read.csv(filepath, sep=",", header=True, schema=schema)


def show_top_hotels_between_couples(dataframe: DataFrame, limit: int) -> DataFrame:
    """Shows top 3 most popular hotels between couples.

    :param dataframe: A Spark Dataframe with hotels
    :type dataframe: Dataframe
    :param limit: Number of hotels to return
    :type limit: int
    :return: A Spark Dataframe with hotels and count  (treat hotel as composite key of
    continent, country and market)
    :rtype: Dataframe
    """
    return (
        dataframe.filter(dataframe["srch_adults_cnt"] == 2)
        .select("hotel_continent", "hotel_country", "hotel_market")
        .groupBy("hotel_continent", "hotel_country", "hotel_market")
        .count()
        .sort("count", ascending=False)
        .limit(limit)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Task_1").getOrCreate()
    filepath = "E:\\PyEducation\\101 Big Data\\expedia-hotel-recommendations\\train.csv"
    show_top_hotels_between_couples(build_hotel_dataframe(spark, filepath), 3).show()
