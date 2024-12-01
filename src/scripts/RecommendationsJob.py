import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
from pyspark.sql.functions import udf
import logging
import math


def main():
    logger = logging.getLogger(__name__)
    events_base_path = sys.argv[1]
    date = sys.argv[2]
    geo_base_path = sys.argv[3]
    output_base_path = sys.argv[4]
    sc = SparkContext.getOrCreate(SparkConf().setAppName(f"RecommendationsJob").set("spark.sql.legacy.timeParserPolicy", "LEGACY"))
    sql = SQLContext(sc)
    
    events_df = sql.read.parquet(f'{events_base_path}/date={date}')

    logger.info('Фильтруем события подписки')
    subscriptions_df = events_df.filter(F.col("event_type") == 'subscription') \
        .select(F.col("event.user").alias("user_id"), F.col("event.subscription_user").alias("channel_id"), F.col("event.datetime").alias("timestamp"), F.col("lat").cast("float"),F.col("lon").cast("float"))
    subscriptions_df = subscriptions_df.select("user_id", "channel_id", F.to_timestamp("timestamp", 'yyyy-MM-dd HH:mm:ss').alias("timestamp"), "lat", "lon")
    messages_df = events_df.filter(F.col("event_type") == 'message') \
        .select(F.col("event.message_from").alias("user_from"), 
                F.col("event.message_to").alias("user_to"),F.col("event.message_ts").alias("timestamp"),F.col("lat").cast("float"),F.col("lon").cast("float"))
    messages_df = messages_df.select("user_from", "user_to", F.to_timestamp("timestamp", 'yyyy-MM-dd HH:mm:ss').alias("timestamp"), "lat", "lon")

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
    logger.info('Cоздание списка городов')
    cities_list = cities_df.select("city", "lat", "lng").collect()
    
    haversine_udf = F.udf(haversine, FloatType())

    logger.info('Создание UDF для определения города')
    get_city_udf = udf(lambda lat, lon: get_city(lat, lon, cities_list), StringType())
    from_uniq_user_df = messages_df.groupBy("user_from").agg(F.max("timestamp").alias("timestamp"), F.first("lat").alias("lat"), F.first("lon").alias("lon"))

    to_uniq_user_df = messages_df.groupBy("user_to").agg(F.max("timestamp").alias("timestamp"), F.first("lat").alias("lat"), F.first("lon").alias("lon"))
    uniq_subscriptions_df =  subscriptions_df.groupBy("user_id").agg(F.max("timestamp").alias("timestamp"), F.first("lat").alias("lat"), F.first("lon").alias("lon"))
    preag_uniq_user_df = from_uniq_user_df.select(F.col("user_from").alias("user_id"),"timestamp","lat","lon").union(to_uniq_user_df.select(F.col("user_to").alias("user_id"),"timestamp","lat","lon"))
    preag_uniq_user_df = preag_uniq_user_df.union(uniq_subscriptions_df)
    uniq_user_df = preag_uniq_user_df.groupBy("user_id").agg(F.max("timestamp").alias("timestamp"), F.first("lat").alias("lat"), F.first("lon").alias("lon"))

    uniq_user_city_df = uniq_user_df.withColumn("city", get_city_udf(F.col("lat"), F.col("lon")))
    uniq_user_city_df = uniq_user_city_df.join(cities_df, uniq_user_city_df.city == cities_df.city, "left").drop(cities_df.city,cities_df.lat,cities_df.lng)
    uniq_user_city_df = uniq_user_city_df.withColumnRenamed("id", "zone_id").withColumnRenamed("timezone", "local_time")

    logger.info('1. Получаем пользователей, подписанных на один канал')
    subscriptions_pairs = subscriptions_df.alias("s1").join(subscriptions_df.alias("s2"), (F.col("s1.channel_id") == F.col("s2.channel_id")) & (F.col("s1.user_id") < F.col("s2.user_id"))) \
        .select(F.col("s1.user_id").alias("user_left"), F.col("s2.user_id").alias("user_right"))

    logger.info('2. Фильтруем пары пользователей по переписке')
    messages_filtered = messages_df.select(
        F.col("user_from").alias("user_left"),
        F.col("user_to").alias("user_right")
    ).union(messages_df.select(
        F.col("user_to").alias("user_left"),
        F.col("user_from").alias("user_right")
    )).distinct()

    logger.info('Находим пары без переписки')
    valid_pairs = subscriptions_pairs.join(messages_filtered, 
                                        (subscriptions_pairs.user_left == messages_filtered.user_left) & 
                                        (subscriptions_pairs.user_right == messages_filtered.user_right), 
                                        "left_anti")


    valid_pairs_with_locations = (valid_pairs 
        .join(uniq_user_city_df.alias("loc_left"), valid_pairs.user_left == F.col("loc_left.user_id")) 
        .join(uniq_user_city_df.alias("loc_right"), valid_pairs.user_right == F.col("loc_right.user_id")) 
        .select(
            valid_pairs.user_left,
            valid_pairs.user_right,
            F.col("loc_left.zone_id").alias("zone_id_left"),
            F.col("loc_left.local_time").alias("local_time_left"),
            F.col("loc_right.zone_id").alias("zone_id_right"),
            F.col("loc_right.local_time").alias("local_time_right"),
            F.col("loc_left.lat").alias("lat_left"),
            F.col("loc_left.lon").alias("lon_left"),
            F.col("loc_right.lat").alias("lat_right"),
            F.col("loc_right.lon").alias("lon_right")
        ))
    valid_pairs_with_locations = valid_pairs_with_locations.filter(F.col('zone_id_left').isNotNull() & F.col('zone_id_right').isNotNull())

    recommendations_df = valid_pairs_with_locations.withColumn("distance", haversine_udf(
            F.col("lat_left"), 
            F.col("lon_left"), 
            F.col("lat_right"), 
            F.col("lon_right")
    )).filter(F.col("distance") <= 1).select(
        "user_left", 
        "user_right",
        F.current_date().alias("processed_dttm"),
        F.col("zone_id_left").alias("zone_id"),
        F.col("local_time_left").alias("local_time"), 
        
    )
    
    recommendations_df.write.mode("overwrite").parquet(f"{output_base_path}_{date}")
    




# Функция для вычисления города по координатам
def get_city(lat, lon, cities):
    if lat is None or lon is None:
        return None  
    min_distance = float('inf')
    city_name = None
    for row in cities:
        distance = haversine(lat, lon, row['lat'], row['lng'])
        if distance < min_distance:
            min_distance = distance
            city_name = row['city']
    return city_name
# Функция Haversine для расчета расстояния между двумя точками

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371  
    return c * r

if __name__ == "__main__":
    main()