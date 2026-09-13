import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .master('local')
    .config("spark.yarn.am.cores", "1")
    .appName("For create datetime")
    .getOrCreate()
)

events = spark.read.json("/user/master/data/events/date=2022-05-31")

events_with_datetime = (
    events
    .withColumn("hour", F.hour("event.datetime"))
    .withColumn("minute", F.minute("event.datetime"))
    .withColumn("second", F.second("event.datetime"))
)
sorted_events = events_with_datetime.orderBy(F.desc("event.datetime"))
sorted_events.show(10, True)
