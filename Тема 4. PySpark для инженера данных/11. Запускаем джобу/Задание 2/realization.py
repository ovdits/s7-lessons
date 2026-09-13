# Замените всё содержимое файла командой запуска из задания.
# Несмотря на расширение .py, сюда нужен текст команды оболочки, а не Python-код.
# Отправка: python submit.py (саму команду клиент не выполняет).
spark-submit --master yarn --deploy-mode cluster lessons/partition.py 2022-05-31 /user/master/data/events /user/username/data/events