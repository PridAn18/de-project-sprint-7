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
dag = DAG("Recommendations_bash_dag",
          schedule_interval='@daily',
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
UsersGeoJob = BashOperator(
    task_id='task_UsersGeo',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster UsersGeoJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/UsersGeo_mart',
        retries=3,
        dag=dag
)

EventsGeoJob = BashOperator(
    task_id='task_EventsGeo',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster EventsGeoJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/EventsGeo_mart',
        retries=3,
        dag=dag
)

RecommendationsJob = BashOperator(
    task_id='task_Recommendations',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster RecommendationsJob.py /user/master/data/geo/events 2022-01-01 /user/pridanova1/data/analytics/geo_2 /user/pridanova1/data/analytics/Recommendations_mart',
        retries=3,
        dag=dag
)

UsersGeoJob >> EventsGeoJob >> RecommendationsJob