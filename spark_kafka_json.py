from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import expr, from_json, col, lower

spark = (
    SparkSession.builder.appName("SparkStreamingKafkaJson")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("city", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("event", StringType(), True),
    ]
)

events = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")  # if kafka is empty, don't fail
    .load()
)

value_df = events.select(
    col("key"), from_json(col("value").cast("string"), schema).alias("value")
)
value_df.show(truncate=False)

tf_df = value_df.selectExpr(
    "value.city",
    "value.domain",
    "value.event as behavior",
)

concat_df = (
    tf_df.withColumn("lower_city", lower(col("city")))
    .withColumn("domain", expr("domain"))
    .withColumn("behavior", expr("behavior"))
    .drop("city")
)

concat_df.show(truncate=False)

output_df = concat_df.selectExpr(
    "null",
    """
    to_json(
        named_struct(
            'lower_city', lower_city,
            'domain', domain,
            'behavior', behavior
        )
    ) AS value
    """.strip(),
)
output_df.show(truncate=False)
