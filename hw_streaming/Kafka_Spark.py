import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


def get_json_schema(spark: SparkSession, host: str, topic: str) -> StructType:
    """Reads kafka topic and returns schema from json

    :param spark: The Spark context associated with this Spark session
    :param host: Host address and port
    :param topic: Kafka topic to read.
    :return: Schema for json
    """
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    headers = json.loads(df.select(col("value").cast("string")).first().value).keys()
    return StructType([StructField(header, StringType()) for header in headers])


def df_from_col_with_jsons(dataframe: DataFrame, schema):
    """Creates spark dataframe from json values in kafka dataframe.

    :param dataframe: Kafka dataframe with JSON format values
    :param scheme: Scheme with keys from JSON
    :return: New dataframe from JSON
    """
    return dataframe.select(from_json("value", schema).alias("json")).select("json.*")


def batch_read_and_write(
    spark: SparkSession, host: str, topic: str, path: str, schema: StructType
) -> None:
    """Reads kafka topic as batch and write to Hadoop.

    :param spark: The Spark context associated with this Spark session
    :param host: Address and port of Kafka Server
    :param topic: Topic name of Kafka to read
    :param path: Path to an output folder
    :param schema: Scheme of JSON
    :return:
    """
    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    df = df_from_col_with_jsons(kafka_df.select(col("value").cast("string")), schema)
    df.write.json(path)


def stream_read_and_write(
    spark: SparkSession, host: str, topic: str, path: str, schema: StructType
) -> None:
    """Reads kafka topic as spark stream and write to Hadoop

    :param spark: The Spark context associated with this Spark session
    :param host: Address and port of Kafka Server
    :param topic: Topic name of Kafka to read
    :param path: Path to an output folder
    :param schema: Scheme of JSON
    :return:
    """
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    df = df_from_col_with_jsons(kafka_df.select(col("value").cast("string")), schema)
    df.writeStream.format("json").option(
        "checkpointLocation", path + "/checkpointLocation"
    ).option("path", path).start().awaitTermination()


if __name__ == "__main__":
    kafka_host = "[::1]:9092"
    kafka_topic = "hotels"
    hdfs_path = "hdfs://0.0.0.0:19000"

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("batch_from_kafka_to_json")
        .getOrCreate()
    )
    schema = get_json_schema(spark, kafka_host, kafka_topic)
    batch_read_and_write(
        spark, kafka_host, kafka_topic, hdfs_path + "hotels_batch", schema
    )
    stream_read_and_write(
        spark, kafka_host, kafka_topic, hdfs_path + "hotels_stream", schema
    )
