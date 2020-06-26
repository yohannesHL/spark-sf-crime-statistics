import os
import sys
import json
import logging
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

service_schema = StructType([
    StructField('crime_id', StringType(), False),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', TimestampType(), True),
    StructField('offense_date', TimestampType(), True),
    StructField('call_date', TimestampType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])

def run_spark_job(spark, filename):

    df = (
        spark 
        .readStream 
        .format('kafka') 
        .option('kafka.bootstrap.servers', KAFKA_BROKER_URL) 
        .option('subscribe', KAFKA_TOPIC_NAME) 
        .option('startingOffsets', 'earliest') 
        .option('maxOffsetPerTrigger', 200) 
        .option('failOnDataLoss', 'false')
        .option('stopGracefullyOnShutdown', 'true')  
        .load() 
    )
    df.printSchema()

    kafka_df = df.select(F.col('value').cast('string'))

    service_table = (
        kafka_df 
        .select(F.from_json(F.col('value'), service_schema).alias('DF')) 
        .select('DF.*')
    )
    service_table.printSchema()

    distinct_table = (
        service_table 
        .dropna(subset=['original_crime_type_name', 'disposition']) 
        .distinct()
        .select('original_crime_type_name', 'disposition')
    )

    # count the number of original crime types
    agg_df = (
        distinct_table
        .groupBy('original_crime_type_name', 'disposition')
        .count()
    )
    agg_df.printSchema()

    query = (
        agg_df
        .writeStream 
        .queryName('count') 
        .trigger(processingTime='5 second') 
        .outputMode('complete')  
        .format('console') 
        .option('truncate', 'false') 
        .option('partitionBy', ['original_crime_type_name', 'disposition'])
        .start()
    )

    query.awaitTermination()

    radio_code_df = (
        spark 
        .read 
        .option('multiline', 'true') 
        .json(filename) 
        .withColumnRenamed('disposition_code', 'disposition')
    )
    join_df = (
        agg_df 
        .join(radio_code_df, 'disposition') 
        .select('original_crime_type_name', 'disposition', 'count', 'description')
    )

    join_query = (
        join_df
        .writeStream
        .queryName('join')
        .outputMode('complete')
        .format('console')
        .option('truncate', 'false')
        .start()
    )
    join_query.awaitTermination()



if __name__ == '__main__':

    if len(sys.argv) < 2:
        raise AssertionError(f'Please provide a path to the json file. \n\t\tUsage: {sys.argv[0]} <filename>')

    filename = os.path.join(Path(__file__).parent, sys.argv[1])

    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config('spark.ui.enabled', 'true')
        .config('spark.ui.port', '4040')   
        .config('spark.sql.shuffle.partitions', 4)
        .config('spark.default.parallelism', 2)
        .appName('KafkaSparkStructuredStreaming')
        .getOrCreate()
    )
    
    run_spark_job(spark, filename)

    spark.stop()

