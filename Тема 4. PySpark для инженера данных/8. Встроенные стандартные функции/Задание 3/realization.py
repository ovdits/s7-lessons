import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .master('local')
    .appName('For filter')
    .getOrCreate()
)

events = spark.read.json('/user/master/data/events/')

events.filter(
    (F.col("date") == "2022-05-25") & 
    (F.col("event_type") == "reaction")
).groupBy("event.reaction_from").count().select(F.max("count")).first()[0]                   
