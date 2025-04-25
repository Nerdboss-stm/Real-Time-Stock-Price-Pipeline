from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, ArrayType, StringType, DoubleType, LongType

# Define Spark Session
spark = SparkSession.builder \
    .appName("StockPriceStreamingConsumer") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming messages
trade_schema = StructType().add("p", DoubleType()).add("s", StringType()).add("t", LongType()).add("v", DoubleType())
message_schema = StructType() \
    .add("type", StringType()) \
    .add("data", ArrayType(trade_schema))

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON messages
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("json_data", from_json("json_str", message_schema)) \
    .selectExpr("json_data.type", "explode(json_data.data) as trade") \
    .select(
        col("type"),
        col("trade.s").alias("symbol"),
        col("trade.p").alias("price"),
        col("trade.v").alias("volume"),
        col("trade.t").alias("timestamp")
    )

# Output to console (for now)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

