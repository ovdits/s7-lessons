import pyspark
from pyspark.sql import SparkSession 
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master('yarn') \
                    .appName('Raw_to_ODS_Partitioned_Job') \
                    .getOrCreate()

raw_path = '/user/master/data/events/'
df_raw = spark.read.json(raw_path)

#df_raw.printSchema()
df_filtered = df_raw.filter(F.col('date') == '2022-05-31')

count = df_filtered.count()
print(f'Найдено строк за 2022-05-31: {count}')

ods_path = '/user/s7811785/data/events'
df_filtered.write.mode('overwrite').partitionBy('date', 'event_type').parquet(ods_path)

df_ods = spark.read.parquet(ods_path)

df_ods.orderBy(F.col('event.datetime').desc()).limit(10).show(truncate=True)

spark.stop()