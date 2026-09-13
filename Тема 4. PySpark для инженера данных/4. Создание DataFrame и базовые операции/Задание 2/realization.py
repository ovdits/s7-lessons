import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = (
    SparkSession.builder
    .master('yarn')
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", 1)
    .config("spark.yarn.am.cores", "1")
    .config("spark.driver.memory", "1g")
    .config("spark.driver.cores", 2)
    .appName("Create DataFrame")
    .getOrCreate()
)

df = spark.read.load(path="/user/master/data/events/date=2022-05-25", format='json')
df.show(10)
