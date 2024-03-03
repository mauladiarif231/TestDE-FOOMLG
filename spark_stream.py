# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
# from google.cloud import bigquery
# import os
#
# # Inisialisasi BigQuery Client
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/testde-foomlg-388ff32a7455.json"
# client = bigquery.Client()
#
# # Konfigurasi BigQuery
# project_id = 'testde-foomlg'
# dataset_id = 'testde'
# table_name = 'created_users'
#
# def create_spark_session():
#     s_conn = None
#
#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages', "com.google.cloud.spark:spark-bigquery-connector-common:0.36.1") \
#             .getOrCreate()
#
#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")
#
#     return s_conn
#
# def connect_to_kafka(spark):
#     kafka_df = None
#     try:
#         kafka_df = spark.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'localhost:9092') \
#             .option('subscribe', 'users_created') \
#             .load()
#         logging.info("Kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"Kafka dataframe could not be created because: {e}")
#
#     return kafka_df
#
# def parse_json_data(kafka_df):
#     schema = StructType([
#         # StructField("id", StringType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])
#
#     parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), schema).alias('data')).select("data.*")
#     return parsed_df
#
# def write_to_bigquery(df, epoch_id):
#     try:
#         df.write \
#             .format('bigquery') \
#             .option('table', f'{project_id}.{dataset_id}.{table_name}') \
#             .option('temporaryGcsBucket', 'your-gcs-bucket') \
#             .mode('append') \
#             .save()
#         logging.info("Data successfully written to BigQuery")
#     except Exception as e:
#         logging.error(f"Failed to write data to BigQuery: {e}")
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#
#     spark = create_spark_session()
#
#     kafka_df = connect_to_kafka(spark)
#
#     if kafka_df:
#         parsed_df = parse_json_data(kafka_df)
#
#         query = parsed_df.writeStream \
#             .outputMode("append") \
#             .foreachBatch(write_to_bigquery) \
#             .start()
#
#         query.awaitTermination()
#     else:
#         logging.error("Failed to connect to Kafka, exiting.")

# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
# import os
#
# # Inisialisasi BigQuery Client
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/testde-foomlg-388ff32a7455.json"
#
# # Konfigurasi BigQuery
# project_id = 'testde-foomlg'
# dataset_id = 'testde'
# table_name = 'created_users'
#
# def create_spark_session():
#     s_conn = None
#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .getOrCreate()
#
#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")
#     return s_conn
#
# def connect_to_kafka(spark):
#     kafka_df = None
#     try:
#         kafka_df = spark.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'localhost:9092') \
#             .option('subscribe', 'users_created') \
#             .load()
#         logging.info("Kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"Kafka dataframe could not be created because: {e}")
#     return kafka_df
#
# def parse_json_data(kafka_df):
#     schema = StructType([
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])
#     parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), schema).alias('data')).select("data.*")
#     return parsed_df
#
# def write_to_csv(df, epoch_id):
#     try:
#         csv_path = f"D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/output-{epoch_id}.csv"
#         df.write \
#             .format("csv") \
#             .option("header", "true") \
#             .mode("append") \
#             .save(csv_path)
#         logging.info(f"Data successfully written to CSV: {csv_path}")
#     except Exception as e:
#         logging.error(f"Failed to write data to CSV: {e}")
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#
#     spark = create_spark_session()
#
#     kafka_df = connect_to_kafka(spark)
#
#     if kafka_df:
#         parsed_df = parse_json_data(kafka_df)
#
#         query = parsed_df.writeStream \
#             .outputMode("append") \
#             .foreachBatch(write_to_csv) \
#             .start()
#
#         query.awaitTermination()
#     else:
#         logging.error("Failed to connect to Kafka, exiting.")


# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
# from google.cloud import bigquery
# from google.cloud.exceptions import NotFound
# import os
#
# # Inisialisasi BigQuery Client
# os.environ["GOOGLE_CLOUD_PROJECT"] = "testde-foomlg"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/testde-foomlg-cad45a74e348.json"
# client = bigquery.Client()
#
# # Konfigurasi BigQuery
# project_id = 'testde-foomlg'
# dataset_id = 'testde'
# table_name = 'created_users'
#
# def create_spark_session():
#     s_conn = None
#
#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.0') \
#             .config('spark.executorEnv.GOOGLE_CLOUD_PROJECT', os.environ['GOOGLE_CLOUD_PROJECT']) \
#             .config('spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS', os.environ['GOOGLE_APPLICATION_CREDENTIALS']) \
#             .getOrCreate()
#
#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")
#
#     return s_conn
#
# def connect_to_kafka(spark):
#     kafka_df = None
#     try:
#         kafka_df = spark.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'localhost:9092') \
#             .option('subscribe', 'users_created') \
#             .load()
#         logging.info("Kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"Kafka dataframe could not be created because: {e}")
#
#     return kafka_df
#
# def parse_json_data(kafka_df):
#     schema = StructType([
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])
#
#     parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), schema).alias('data')).select("data.*")
#     return parsed_df
#
# def create_table_if_not_exists(client, project_id, dataset_id, table_name, schema):
#     try:
#         table_ref = client.dataset(dataset_id).table(table_name)
#         client.get_table(table_ref)
#     except NotFound:
#         table_ref = bigquery.Table(f"{project_id}.{dataset_id}.{table_name}", schema=schema)
#         table = client.create_table(table_ref)
#         logging.info(f"Table {table_name} created successfully in BigQuery.")
#     except Exception as e:
#         logging.error(f"Error while creating table: {e}")
#
#
# def write_to_bigquery(df, epoch_id):
#     try:
#         schema = [
#             bigquery.SchemaField("first_name", "STRING"),
#             bigquery.SchemaField("last_name", "STRING"),
#             bigquery.SchemaField("gender", "STRING"),
#             bigquery.SchemaField("address", "STRING"),
#             bigquery.SchemaField("post_code", "STRING"),
#             bigquery.SchemaField("email", "STRING"),
#             bigquery.SchemaField("username", "STRING"),
#             bigquery.SchemaField("registered_date", "STRING"),
#             bigquery.SchemaField("phone", "STRING"),
#             bigquery.SchemaField("picture", "STRING")
#         ]
#
#         create_table_if_not_exists(client, project_id, dataset_id, table_name, schema)
#
#         # Menulis data streaming ke BigQuery
#         df.write \
#             .format('bigquery') \
#             .option('table', f'{project_id}.{dataset_id}.{table_name}') \
#             .mode('append') \
#             .save()
#
#         logging.info("Data successfully written to BigQuery")
#     except Exception as e:
#         logging.error(f"Failed to write data to BigQuery: {e}")
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#
#     spark = create_spark_session()
#
#     kafka_df = connect_to_kafka(spark)
#
#     if kafka_df:
#         parsed_df = parse_json_data(kafka_df)
#
#         query = parsed_df.writeStream \
#             .outputMode("append") \
#             .foreachBatch(write_to_bigquery) \
#             .start()
#
#         query.awaitTermination()
#     else:
#         logging.error("Failed to connect to Kafka, exiting.")

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

# Inisialisasi BigQuery Client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/testde-foomlg-cad45a74e348.json"

# Konfigurasi BigQuery
project_id = 'testde-foomlg'
dataset_id = 'testde'
table_name = 'created_users'

def create_spark_session():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
            .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', 'D:/Mnet-Maul/Data Engineering/Test Foom Lab Global/DE-Test New/testde-foomlg-cad45a74e348.json') \
            .config('spark.hadoop.fs.gs.project.id', project_id) \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None

def connect_to_kafka(spark):
    kafka_df = None
    try:
        kafka_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return kafka_df

def parse_json_data(kafka_df):
    schema = StructType([
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

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return parsed_df

def create_table_if_not_exists(client, dataset_id, table_name, schema):
    try:
        table_ref = client.dataset(dataset_id).table(table_name)
        client.get_table(table_ref)
    except NotFound:
        table_ref = bigquery.Table(f"{project_id}.{dataset_id}.{table_name}", schema=schema)
        client.create_table(table_ref)
        logging.info(f"Table {table_name} created successfully in BigQuery.")
    except Exception as e:
        logging.error(f"Error while creating table: {e}")

def write_to_bigquery(df, epoch_id):
    try:
        schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("post_code", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("username", "STRING"),
            bigquery.SchemaField("registered_date", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("picture", "STRING")
        ]

        # Inisialisasi BigQuery Client
        client = bigquery.Client(project=project_id)

        create_table_if_not_exists(client, dataset_id, table_name, schema)

        # Menulis data streaming ke BigQuery
        df.write \
            .format('bigquery') \
            .option('table', f'{project_id}.{dataset_id}.{table_name}') \
            .option('temporaryGcsBucket', 'nama_bucket_gcs_anda') \
            .mode('append') \
            .save()

        logging.info("Data successfully written to BigQuery")
    except Exception as e:
        logging.error(f"Failed to write data to BigQuery: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = create_spark_session()

    kafka_df = connect_to_kafka(spark)

    if kafka_df:
        parsed_df = parse_json_data(kafka_df)

        query = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_bigquery) \
            .start()

        query.awaitTermination()
    else:
        logging.error("Failed to connect to Kafka, exiting.")

