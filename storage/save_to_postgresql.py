from pyspark.sql import SparkSession
from pyspark.sql.functions import from_avro, col

# Avro schema string
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

# Set your credentials
POSTGRES_URL = "jdbc:postgresql://localhost:5432/stock_stream"
POSTGRES_PROPERTIES = {
    "user": "nerdboss-stm",
    "password": "StockStream123!",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("StockToPostgresSink") \
    .master("local[*]") \
    .config("spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
        "org.apache.spark:spark-avro_2.12:3.1.2,"
        "org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_stream_avro") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.select(from_avro(col("value"), stock_trade_schema_json).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        col("data.volume"),
        col("data.timestamp")
    )

def write_to_postgres(batch_df, epoch_id):
    batch_df.write.jdbc(
        url=POSTGRES_URL,
        table="stock_prices",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )

query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()

