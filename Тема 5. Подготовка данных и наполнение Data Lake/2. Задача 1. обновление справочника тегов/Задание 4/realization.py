from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import sys


def get_input_paths(base_path, date_str, depth):
    current_date = datetime.strptime(date_str, '%Y-%m-%d')
    base_path = '/user/s7811785/data/events'
    paths = []

    for i in range(depth):
        target_date = current_date - timedelta(days=i)
        formatted_date = target_date.strftime('%Y-%m-%d')
        full_path = f'{base_path}/date={formatted_date}/event_type=message'
        paths.append(full_path)

    return paths


def main(date, depth, threshold, input_path, verified_tags_path, output_path):
    appname = f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{threshold}"
    spark = SparkSession.builder \
        .appName(appname) \
        .getOrCreate()

    input_paths_list = get_input_paths(input_path, date, depth)
    print(f'Читаем данные из {len(input_paths_list)} партиций.')

    messages = spark.read.parquet(*input_paths_list)

    all_tags = messages.where(
        'event.message_channel_to is not null'
    ).selectExpr(
        'event.message_from as user',
        'explode(event.tags) as tag'
    ).groupBy('tag').agg(
        F.expr('count(distinct user) as suggested_count')
    ).where(f'suggested_count >= {threshold}')

    verified_tags = spark.read.parquet(verified_tags_path)

    candidates = all_tags.join(verified_tags, 'tag', 'left_anti')

    candidates.write.mode('overwrite').parquet(output_path)

    spark.stop()


if __name__ == '__main__':
    if len(sys.argv) != 7:
        print('Usage: spark-submit verified_tags_candidates.py <date> <depth> <threshold> <input_path> <verified_tags_path> <output_path>')
        sys.exit(1)

    date = sys.argv[1]
    depth = int(sys.argv[2])
    threshold = int(sys.argv[3])
    input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    output_path = sys.argv[6]

    main(date, depth, threshold, input_path, verified_tags_path, output_path)