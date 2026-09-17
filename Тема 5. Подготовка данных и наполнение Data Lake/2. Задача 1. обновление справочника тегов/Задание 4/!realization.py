import sys
import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


def main():
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    suggested_cutoff = int(sys.argv[3])
    base_input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    base_output_path = sys.argv[6]

    spark = SparkSession.builder \
        .appName(f'VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}') \
        .getOrCreate()

    current_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    paths = []
    for i in range(days_count):
        target_date = current_date - datetime.timedelta(days=i)
        formatted = target_date.strftime('%Y-%m-%d')
        paths.append(f'{base_input_path}/date={formatted}/event_type=message')

    messages = spark.read.parquet(*paths)
    verified_tags = spark.read.parquet(verified_tags_path)

    candidates = find_candidates(messages, verified_tags, suggested_cutoff)

    candidates.write \
        .mode('overwrite') \
        .parquet(base_output_path)

    spark.stop()


def find_candidates(messages, verified_tags, suggested_cutoff):
    all_tags = messages \
        .filter(F.col('event.message_channel_to').isNotNull()) \
        .selectExpr(
            'event.message_from as user',
            'explode(event.tags) as tag'
        ) \
        .groupBy('tag') \
        .agg(
            F.countDistinct('user').alias('suggested_count')
        ) \
        .filter(F.col('suggested_count') >= suggested_cutoff)

    candidates = all_tags.join(
        verified_tags,
        on='tag',
        how='left_anti'
    )

    return candidates


if __name__ == "__main__":
    main()