import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
                    .master("local") \
                    .appName("Copy_events_from_master_data") \
                    .getOrCreate()

events = spark.read.json("/user/master/data/events")
events.write \
        .partitionBy("date", "event_type") \
        .mode("overwrite") \
        .parquet("/user/s7811785/data/events")
