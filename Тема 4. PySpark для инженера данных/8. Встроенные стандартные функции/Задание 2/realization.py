import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .master('local')
    .config('spark.yarn.am.cores', '1')
    .appName('For filter')
    .getOrCreate()
)

events = spark.read.json('/user/master/data/events/date=2022-05-31')

events.filter(F.col('event.message_to').isNotNull()).count()