from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import requests
import pandas as pd
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure
import telegram
import io
import sys
import os

# Импортируем все необходимые библиотеки

connection1 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Задаем словарь для подключения к БД Clickhouse

default_args = {
    'owner': 'i-gromov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
}

# Задаем словарь с основными параметрами для настройки DAG'а

schedule_interval = '*/15 * * * *'

# Даг будет срабатывать каждые 15 минут

bot_token = '5693821822:AAHnPdjmitf-R9CciISIyjMEUW7Ljas_fb0'
bot = telegram.Bot(token = bot_token)
chat_id = -634869587

# Для создания DAG'а был создан бот, имеющий следующий токен, также задан id чата, в которой будет приходить сообщение от бота

# Feed active users
query1 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
COUNT(DISTINCT user_id) as feed_users_count
FROM simulator_20221220.feed_actions
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

# Messages active users
query2 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
COUNT(DISTINCT user_id) as message_users_count
FROM (SELECT user_id,
time
FROM simulator_20221220.message_actions
LEFT JOIN simulator_20221220.feed_actions ON simulator_20221220.feed_actions.user_id = simulator_20221220.message_actions.user_id) AS virtual_table
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

# Likes
query3 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
countIf(user_id, action='like') AS likes
FROM simulator_20221220.feed_actions
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

# Views
query4 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
countIf(user_id, action='view') AS views
FROM simulator_20221220.feed_actions
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

#CTR
query5 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
FROM simulator_20221220.feed_actions
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

# Count Messages
query6 = """SELECT toStartOfFifteenMinutes(time) as ts, 
toDate(ts) as date, 
formatDateTime(ts, '%R') as hm, 
count(user_id) AS messages_count
FROM (SELECT user_id,
time
FROM simulator_20221220.message_actions
LEFT JOIN simulator_20221220.feed_actions ON simulator_20221220.feed_actions.user_id = simulator_20221220.message_actions.user_id) AS virtual_table
WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts"""

df1 = pandahouse.read_clickhouse(query1, connection = connection1)
df2 = pandahouse.read_clickhouse(query2, connection = connection1)
df3 = pandahouse.read_clickhouse(query3, connection = connection1)
df4 = pandahouse.read_clickhouse(query4, connection = connection1)
df5 = pandahouse.read_clickhouse(query5, connection = connection1)
df6 = pandahouse.read_clickhouse(query6, connection = connection1)

# Были сделаны запросы к БД Clickhouse и сформированы на их основе датафреймы

def check_anomaly(df, metric, threshold = 0.3):
    current_ts = df['ts'].max()
    day_ago_ts = current_ts - pd.DateOffset(days = 1)

    current_value = df[df['ts'] == current_ts][metric].iloc[0]
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0]

    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)
        
    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, current_value, diff

# Данная функция принимает на вход датафрейм, название метрики из датафрейма и значение отклонения, она возвращает истину, если отклонение между величинами за текущий момент и день назад больше или меньше заданного отклонения

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def i_gromov_dag_task_8():
    @task()
    def checking_anomalies_1(chat_id, df1):
        metric = 'feed_users_count'
        is_alert, current_value, diff = check_anomaly(df1, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df1.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений количества пользователей ленты новостей')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

# Данный таск запускает функцию check_anomaly по количеству пользователей ленты новостей         
            
    @task()
    def checking_anomalies_2(chat_id, df2):
        metric = 'message_users_count'
        is_alert, current_value, diff = check_anomaly(df2, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df2.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений количества пользователей сообщений')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

 # Данный таск запускает функцию check_anomaly по количеству пользователей сообщений            
            
    @task()
    def checking_anomalies_3(chat_id, df3):
        metric = 'likes'
        is_alert, current_value, diff = check_anomaly(df3, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df3.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений количества лайков')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

 # Данный таск запускает функцию check_anomaly по количеству лайков               

    @task()
    def checking_anomalies_4(chat_id, df4):
        metric = 'views'
        is_alert, current_value, diff = check_anomaly(df4, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df4.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений количества просмотров')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

 # Данный таск запускает функцию check_anomaly по количеству просмотров              

    @task()
    def checking_anomalies_5(chat_id, df5):
        metric = 'CTR'
        is_alert, current_value, diff = check_anomaly(df5, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df5.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений CTR')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

 # Данный таск запускает функцию check_anomaly по метрике CTR             
            
    @task()
    def checking_anomalies_6(chat_id, df6):
        metric = 'messages_count'
        is_alert, current_value, diff = check_anomaly(df6, metric, threshold = 0.1)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric = metric,
                                                                                                                     current_value = current_value,
                                                                                                                     diff = diff)
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(
            data = df6.sort_values(by = ['date', 'hm']), 
            x = "hm", y = metric,
            hue= "date")
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title('Динамика изменений количества сообщений')
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

 # Данный таск запускает функцию check_anomaly по количеству сообщений           
               
    checking_anomalies_1(chat_id, df1)
    checking_anomalies_2(chat_id, df2)
    checking_anomalies_3(chat_id, df3)
    checking_anomalies_4(chat_id, df4)
    checking_anomalies_5(chat_id, df5)
    checking_anomalies_6(chat_id, df6)

i_gromov_dag_task_8 = i_gromov_dag_task_8()