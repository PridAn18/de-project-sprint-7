# Проект 7-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 7-го спринта

Проектная работа по организации Data Lake: HDFS, PySpark, AirFlow

### Структура HDFS

- **events**: /user/master/data/geo/events Директория содержит исходные данные событий, которые включают новые поля: широта и долгота.

- **geo_2**: Директория содержит файл geo_2.csv. Этот файл был загружен из локальной системы в HDFS с помощью команды copyFromLocal.
Данные с геопозицией городов и timezones хранится в /user/pridanova1/data/analytics/geo_2

Витрины будут храниться в слое /user/pridanova1/data/analytics/

### PySpark Jobs

Постриение 3 витрин: UsersGeo_mart, EventsGeo_mart, Recommendations_mart

- **UsersGeo_mart**: UsersGeoJob строит витрину в разрезе пользователей с полями:\
**user_id** — идентификатор пользователя;\
**act_city** — актуальный адрес. Это город, из которого было отправлено последнее сообщение;\
**home_city** — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней;\
**travel_count** — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение;\
**travel_array** — список городов в порядке посещения;\
**local_time** — местное время; 


- **EventsGeo_mart**: EventsGeoJob строит витрину в разрезе пользователей с полями:\
**month** — месяц расчёта;\
**week** — неделя расчёта;\
**zone_id** — идентификатор зоны (города);\
**week_message** — количество сообщений за неделю;\
**week_reaction** — количество реакций за неделю;\
**week_subscription** — количество подписок за неделю;\
**week_user** — количество регистраций за неделю;\
**month_message** — количество сообщений за месяц;\
**month_reaction** — количество реакций за месяц;\
**month_subscription** — количество подписок за месяц;\
**month_user** — количество регистраций за месяц.

- **Recommendations_mart**: RecommendationsJob строит витрину в разрезе пользователей с полями:\
**user_left** — первый пользователь;\
**user_right** — второй пользователь;\
**processed_dttm** — дата расчёта витрины;\
**zone_id** — идентификатор зоны (города);\
**local_time** — локальное время.



### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` DAG Airflow;
- `/src/sql` JOBs для обработки данных.

## Формат данных

Все данные хранятся в формате Parquet, который обеспечивает эффективное хранение и быструю обработку. Исключение составляет файл geo_2.csv, который хранится в текстовом формате CSV для удобства работы с геолокационными данными.

