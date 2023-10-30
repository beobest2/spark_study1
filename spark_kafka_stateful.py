from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr, from_json, col, lower

spark = (
    SparkSession.builder.appName("SparkStreamingKafkaJsonStream")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("amount", IntegerType(), True),
    ]
)

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pos")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")  # if kafka is empty, don't fail
    .load()
)

value_df = events.select(
    col("key"), from_json(col("value").cast("string"), schema).alias("value")
)

tf_df = value_df.selectExpr(
    "value.product_id",
    "value.amount",
)

total_df = (
    tf_df.select("product_id", "amount")
    .groupBy("product_id")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount")
)

query = total_df.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
