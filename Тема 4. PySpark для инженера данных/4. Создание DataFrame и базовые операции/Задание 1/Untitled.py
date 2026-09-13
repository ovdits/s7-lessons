
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

data = [
    ('Max', 55),
    ('Yan', 53),
    ('Dmitry', 54),
    ('Ann', 25)
]

columns = ['Name','Age']

df = spark.createDataFrame(data, columns) 

df.printSchema()
