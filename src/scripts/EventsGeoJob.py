import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import udf
import logging
import math

def main():
    logger = logging.getLogger(__name__)
    events_base_path = sys.argv[1]
    date = sys.argv[2]
    geo_base_path = sys.argv[3]
    output_base_path = sys.argv[4]
    

    sc = SparkContext.getOrCreate(SparkConf().setAppName(f"EventsGeoJob").set("spark.sql.legacy.timeParserPolicy", "LEGACY"))
    sql = SQLContext(sc)
    events_df = sql.read.parquet(f'{events_base_path}/date={date}')

    logger.info('Шаг 1: Извлекаем user_id и timestamp из структуры event и добавляем их в DataFrame')
    events_with_user_info = events_df.withColumn(
        "user_id",
        F.when(F.col("event_type") == "subscription", F.col("event.user"))
        .when(F.col("event_type") == "reaction", F.col("event.reaction_from"))
        .when(F.col("event_type") == "message", F.col("event.message_from"))
    ).withColumn(
        "timestamp",
        F.when(F.col("event_type") == "message", F.col("event.message_ts"))
        .when(F.col("event_type") == "reaction", F.col("event.datetime"))
        .when(F.col("event_type") == "subscription", F.col("event.datetime"))
    )
    events_with_user_info = events_with_user_info.select("user_id", "event_type", F.to_timestamp("timestamp", 'yyyy-MM-dd HH:mm:ss').alias("timestamp"), "lat", "lon")
    logger.info('Шаг 2: Определяем первое сообщение для каждого пользователя')
    first_messages_df = events_with_user_info.filter(F.col("event_type") == "message").groupBy("user_id").agg(
        F.min("timestamp").alias("first_message_ts")
    )
    last_messages_df = events_with_user_info.filter(F.col("event_type") == "message").groupBy("user_id").agg(
        F.max("timestamp").alias("last_message_ts"),
        F.first("lat").alias("last_lat"),
        F.first("lon").alias("last_lng")
    )
    logger.info('Шаг 3: Создаем DataFrame для регистраций на основе первых сообщений')
    registration_df = first_messages_df.join(
        last_messages_df,
        on="user_id",
        how="inner"
    ).select(
        "user_id",
        F.when(F.col("user_id").isNotNull(), F.lit("registration")).alias("event_type"),
        "first_message_ts",
        "last_lat",
        "last_lng"
    )
    logger.info('Шаг 4: Добавляем недостающие колонки в registration_df')
    registration_df = registration_df.withColumnRenamed("first_message_ts", "timestamp") \
                                    .withColumnRenamed("last_lat", "lat") \
                                    .withColumnRenamed("last_lng", "lon")
    events_with_user_info=events_with_user_info.select("user_id", "event_type", "timestamp", "lat", "lon")
    final_events_df = events_with_user_info.union(registration_df)
    logger.info('Создание списка городов')
    schema = StructType([
    StructField("_c0", IntegerType(), True),  # id
    StructField("_c1", StringType(), True),   # city
    StructField("_c2", DoubleType(), True),   # lat
    StructField("_c3", DoubleType(), True),   # lng
    StructField("_c4", StringType(), True)    # timezone
    ])
    cities_df = sql.read.csv(f'{geo_base_path}', schema=schema)
    cities_df = cities_df.select(F.col('_c0').alias("id"),F.col('_c1').alias("city"),F.col('_c2').alias("lat"),F.col('_c3').alias("lng"),F.col('_c4').alias("timezone"))
    cities_list = cities_df.select("city", "lat", "lng").collect()
    
    logger.info('Создание UDF для определения города')
    get_city_udf = udf(lambda lat, lon: get_city(lat, lon, cities_list), StringType())
    logger.info('Добавление колонки города в DataFrame событий')
    events_with_city_df = final_events_df.withColumn("city", get_city_udf(F.col("lat").cast("float"), F.col("lon").cast("float")))
    events_with_city_df = events_with_city_df.withColumn("month", F.month(F.col("timestamp"))).withColumn("week", F.weekofyear(F.col("timestamp")))
    events_with_city_df = events_with_city_df.join(cities_df, events_with_city_df.city == cities_df.city, "left").drop(cities_df.city,cities_df.lat,cities_df.lng,events_with_city_df.timestamp,events_with_city_df.lat,events_with_city_df.lon,events_with_city_df.city,cities_df.timezone)
    result_df = events_with_city_df.select(F.col('id').alias("zone_id"), 'month', 'week', 'event_type')
    result_df.show(10)
    result_df = events_with_city_df.groupBy("month", "week", "id").agg(
        F.count(F.when(F.col("event_type") == "message", True)).alias("week_message"),
        F.count(F.when(F.col("event_type") == "reaction", True)).alias("week_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", True)).alias("week_subscription"),
        F.count(F.when(F.col("event_type") == "registration", True)).alias("week_user"),
        F.count(F.when(F.col("event_type") == "message", True)).alias("month_message"),
        F.count(F.when(F.col("event_type") == "reaction", True)).alias("month_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", True)).alias("month_subscription"),
        F.count(F.when(F.col("event_type") == "registration", True)).alias("month_user")
    ).orderBy("month", "week", "id")
    result_df.write.mode("overwrite").parquet(f"{output_base_path}_{date}")





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