import airflow
import os 
# импортируем модуль os, который даёт возможность работы с ОС
# указание os.environ[…] настраивает окружение

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

# прописываем пути
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval='@daily',
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
UsersGeoJob = BashOperator(
    task_id='task_1',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster UsersGeoJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/UsersGeo_mart',
        retries=3,
        dag=dag
)

EventssGeoJob = BashOperator(
    task_id='task_2',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster EventssGeoJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/EventsGeo_mart',
        retries=3,
        dag=dag
)

RecommendationsJob = BashOperator(
    task_id='task_3',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster RecommendationsJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/Recommendations_mart',
        retries=3,
        dag=dag
)

UsersGeoJob >> EventssGeoJob >> RecommendationsJob