from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("tags_candidates") \
    .getOrCreate()


def get_input_paths(date_str, depth):
    current_date = datetime.strptime(date_str, '%Y-%m-%d')
    base_path = '/user/s7811785/data/events'
    paths = []

    for i in range(depth):
        target_date = current_date - timedelta(days=i)
        formatted_date = target_date.strftime('%Y-%m-%d')
        full_path = f'{base_path}/date={formatted_date}/event_type=message'
        paths.append(full_path)

    return paths


date_str = '2022-05-31'
depth = 84
input_paths_list = get_input_paths(date_str, depth)

print(f'Читаем данные из {len(input_paths_list)} партиций.')

messages = spark.read.parquet(*input_paths_list)

all_tags = messages.where(
    'event.message_channel_to is not null'
).selectExpr(
    'event.message_from as user',
    'explode(event.tags) as tag'
).groupBy('tag').agg(
    F.expr('count(distinct user) as suggested_count')
).where('suggested_count >= 100')

verified_tags = spark.read.parquet(
    '/user/master/data/snapshots/tags_verified/actual'
)

candidates = all_tags.join(verified_tags, 'tag', 'left_anti')

candidates.write.parquet(
    '/user/s7811785/data/analytics/candidates_d84_pyspark'
)
