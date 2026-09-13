"""Заготовка решения. Замените TODO своим кодом перед отправкой."""


from pyspark.sql import functions as F
from datetime import datetime, timedelta


def input_paths(date, depth):
    current_date = datetime.strptime(date, '%Y-%m-%d')
    base_path = '/user/s7811785/data/events'
    paths = []

    for i in range(depth):
        target_date = current_date - timedelta(days=i)
        formatted_date = target_date.strftime('%Y-%m-%d')
        full_path = f'{base_path}/date={formatted_date}/event_type=message'
        paths.append(full_path)

    return paths

    raise NotImplementedError("Дополните решение по условию задания")
