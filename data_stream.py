import os
from pathlib import Path
import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

service_schema = StructType([
    StructField("crime_id", IntegerType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", IntegerType(), True),
    StructField("address_type", StringType(), True)    
])

def run_spark_job(spark, filename):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true")  \
        .load() 

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(F.from_json(F.col('value'), service_schema).alias("DF")) \
        .select("DF.*")

    distinct_table = service_table \
        .dropna(subset=["original_crime_type_name", "disposition"]) \
        .distinct() \
        .select("original_crime_type_name", "disposition") \
        .groupBy("original_crime_type_name", "disposition") \


    # count the number of original crime type
    agg_df = distinct_table.count() 

    query = agg_df \
            .writeStream \
            .trigger(processingTime='100 seconds') \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    query.awaitTermination()

    radio_code_df = spark.read.option("multiline", "true")
                    .json(filename)
                    .withColumnRenamed("disposition_code", "disposition")

    join_df = agg_df \
        .join(radio_code_df, 'disposition') 
        .select("original_crime_type_name", "disposition", "count", "description")

    join_query = join_df \
        .writeStream \
        .queryName("join") \
        .outputMode("complete") \
        .trigger(once=True) \
        .format("console") \
        .option("truncate", "false") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)


    if len(sys.argv) < 2:
        raise AssertionError(f'Please provide a path to the json file. \n\t\tUsage: {sys.argv[0]} <filename>')

    filename = os.join(Path(__file__).parent, sys.argv[1])
    logger.debug(f'Using json source file: {filename}')

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.proxyBase", "") \
        .appName("KafkaSparkStructuredStreaming3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark started")

    run_spark_job(spark, filename)

    spark.stop()
