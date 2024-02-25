from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr, from_json, col, lower, to_timestamp, window

"""
Tumbling time window : no overlap
"""

spark = (
    SparkSession.builder.appName("SparkStreaming")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("create_date", StringType(), True),
        StructField("amount", IntegerType(), True),
    ]
)

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "timeseries")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")  # if kafka is empty, don't fail
    .load()
)

value_df = events.select(
    col("key"), from_json(col("value").cast("string"), schema).alias("value")
)

timestamp_format = "yyyy-MM-dd HH:mm:ss"
tf_df = value_df.select("value.*").withColumn(
    "create_date", to_timestamp("create_date", timestamp_format)
)

window_duration = "5 minutes"
window_agg_df = (
    tf_df.groupBy(window(col("create_date"), window_duration))
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount")
)
"""
sliding_duration = "2 minutes"
window_agg_df = (
    tf_df.groupBy(window(col("create_date"), window_duration, sliding_duration))
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount")
)
"""

query = (
    window_agg_df.writeStream.format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .outputMode("complete")
    .start()
)
query.awaitTermination()
