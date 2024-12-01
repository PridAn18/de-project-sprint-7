import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import udf
import logging
import math


def main():
    logger = logging.getLogger(__name__)
    events_base_path = sys.argv[1]
    date = sys.argv[2]
    geo_base_path = sys.argv[3]
    output_base_path = sys.argv[4]
    sc = SparkContext.getOrCreate(SparkConf().setAppName(f"UsersGeoJob").set("spark.sql.legacy.timeParserPolicy", "LEGACY"))
    sql = SQLContext(sc)

    events_df = sql.read.parquet(f'{events_base_path}/date={date}')

    logger.info('Добавление user_id в events_df и приведение координат к типу float')
    events_with_user_id = events_df.withColumn(
        "user_id",
        F.when(F.col("event_type") == "subscription", F.col("event.user"))
        .when(F.col("event_type") == "reaction", F.col("event.reaction_from"))
        .when(F.col("event_type") == "message", F.col("event.message_from"))
    ).withColumn(
        "lat", F.col("lat").cast("float")
    ).withColumn(
        "lng", F.col("lon").cast("float")
    )
    events_with_user_id = events_with_user_id.filter(F.col("lat").isNotNull() & F.col("lng").isNotNull())

    logger.info('Чтение нового файла с городами и таймзонами')
    schema = StructType([
    StructField("_c0", IntegerType(), True),  # id
    StructField("_c1", StringType(), True),   # city
    StructField("_c2", DoubleType(), True),   # lat
    StructField("_c3", DoubleType(), True),   # lng
    StructField("_c4", StringType(), True)    # timezone
    ])
    cities_df = sql.read.csv(f'{geo_base_path}', schema=schema)
    cities_df = cities_df.select(F.col('_c0').alias("id"),F.col('_c1').alias("city"),F.col('_c2').alias("lat"),F.col('_c3').alias("lng"),F.col('_c4').alias("timezone"))
    logger.info('Создание списка городов')
    cities_list = cities_df.select("city", "lat", "lng").collect()
    
    logger.info('Создание UDF для определения города')
    get_city_udf = udf(lambda lat, lon: get_city(lat, lon, cities_list), StringType())

    logger.info('Добавление колонки города в DataFrame событий')
    events_with_city_df = events_with_user_id.withColumn("city", get_city_udf(F.col("lat"), F.col("lng")))

    logger.info("Создание временной метки для определения последовательности")
    events_with_city_df = events_with_city_df.withColumn("event_datetime", 
        F.when(F.col("event_type") == "message", F.col("event.message_ts"))
        .otherwise(F.col("event.datetime")))

    logger.info("Определение окна для нумерации событий")
    window_spec = Window.partitionBy("user_id").orderBy("event_datetime")
    events_with_city_df = events_with_city_df.withColumn("event_number", F.row_number().over(window_spec))

    logger.info("Подсчет количества дней для домашнего города")
    home_city_df = events_with_city_df.groupBy("user_id", "city").agg(
        F.countDistinct(F.date_trunc("day", "event_datetime")).alias("days_count")
    ).filter(F.col("days_count") >= 27)

    logger.info("Определение домашнего города как последнего города, где пользователь был больше 27 дней")
    home_city_final_df = home_city_df.groupBy("user_id").agg(
        F.last("city").alias("home_city")
    )

    logger.info("Получение активного города и последнего времени события")
    act_city_df = events_with_city_df.groupBy("user_id").agg(
        F.last("city").alias("act_city"),
        F.last("event_datetime").alias("last_event_time")
    )

    logger.info("Подсчет количества путешествий и создание массива городов")
    travel_count_df = events_with_city_df.groupBy("user_id").agg(
        F.countDistinct("city").alias("travel_count"),
        F.collect_list("city").alias("travel_array")
    )

    logger.info("Присоединение информации о таймзоне к активному городу, выбираем только колонку timezone")
    timezones_df = cities_df.select("city", "timezone")

    local_time_df = act_city_df.join(timezones_df, act_city_df.act_city == timezones_df.city, "left").join(events_with_city_df, "user_id").select(
        "user_id",
        "act_city",
        "last_event_time",
        "timezone"
    ).withColumn("local_time", F.from_utc_timestamp(F.col("last_event_time"), F.col('timezone')))

    logger.info("Объединение всех данных в финальный DataFrame")
    final_vitrine_df = (home_city_final_df.join(travel_count_df, "user_id", "outer")
                        .join(local_time_df, "user_id", "outer") 
                        .select(
                            "user_id",
                            "act_city",
                            "home_city",
                            "travel_count",
                            "travel_array",
                            "local_time"
                        ))
    final_vitrine_df.write.mode("overwrite").parquet(f"{output_base_path}_{date}")
    

def get_city(lat, lon, cities):
    if lat is None or lon is None:
        return None  # Возвращаем None, если координаты отсутствуют
    min_distance = float('inf')
    city_name = None
    for row in cities:
        distance = haversine(lat, lon, row['lat'], row['lng'])
        if distance < min_distance:
            min_distance = distance
            city_name = row['city']
    return city_name

# Функция Haversine для расчета расстояния между двумя точками
def haversine(lat1, lng1, lat2, lng2):
    dLat = (lat2 - lat1) * math.pi / 180.0
    dLng = (lng2 - lng1) * math.pi / 180.0
    lat1 = (lat1) * math.pi / 180.0
    lat2 = (lat2) * math.pi / 180.0
    a = (pow(math.sin(dLat / 2), 2) +
         pow(math.sin(dLng / 2), 2) *
             math.cos(lat1) * math.cos(lat2))
    rad = 6371
    c = 2 * math.asin(math.sqrt(a))
    return rad * c

if __name__ == "__main__":
    main()