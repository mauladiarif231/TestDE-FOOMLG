import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName('SparkDataStreaming') \
    .getOrCreate()

# Konfigurasi Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'users_created'

# Skema data JSON
schema = StructType([
    StructField("id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("post_code", StringType(), False),
    StructField("email", StringType(), False),
    StructField("username", StringType(), False),
    StructField("registered_date", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("picture", StringType(), False)
])

def process_kafka_stream():
    try:
        # Baca stream Kafka
        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
            .option('subscribe', kafka_topic) \
            .load()

        # Parse data JSON
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")

        # Tulis ke BigQuery (contoh)
        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()
    except Exception as e:
        logging.error(f"Error while processing Kafka stream: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_kafka_stream()
