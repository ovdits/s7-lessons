import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *


df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

df.write.partitionBy("channel_type").mode("append").parquet("/user/s7811785/analytics/test")

spark.read.parquet("/user/s7811785/analytics/test").select("channel_type").orderBy("channel_type").distinct().show()