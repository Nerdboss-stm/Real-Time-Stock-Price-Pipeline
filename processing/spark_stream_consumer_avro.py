from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, ArrayType, StringType, DoubleType, LongType

# Define the same Avro Schema inline
stock_trade_schema_json = """
{
  "namespace": "stock.price",
  "type": "record",
  "name": "StockTrade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "volume", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Start Spark Session
spark = SparkSession.builder \
    .appName("StockPriceConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-avro_2.12:3.1.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read Avro data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_stream_avro") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize Avro payload
df_parsed = df_raw.select(from_avro(col("value"), stock_trade_schema_json).alias("data")) \
    .select(
        col("data.symbol").alias("symbol"),
        col("data.price").alias("price"),
        col("data.volume").alias("volume"),
        col("data.timestamp").alias("timestamp")
    )

# Output to console (for now)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

