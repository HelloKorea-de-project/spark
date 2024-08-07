import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType
import random
from datetime import timedelta
from collections import defaultdict

# Glue context 추출, Spark session 초기화
sc = SparkContext.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 데이터 행 수 및 파티션 개수 설정
repeat_count = 300000000
num_partitions = 200

df1 = spark.read.parquet('s3://hellokorea-extra-data-zone/source/web_log/airline_search_log.parquet/').select("session_id", "searched_ts", "user_id", "depart_time", "depart_airport_code").distinct()
search_list = df1.sample(False, 0.02).collect()
bc = sc.broadcast(search_list)
searches = bc.value

df2 = spark.read.csv('s3://hellokorea-external-zone/source/flight/cheapest_flight_to_ICN_updated_2024-08-01.csv000').select("_c0","_c1","_c6")
flight_list = df2.collect()
flight_dict = defaultdict(list)
for id, dep_code, dep_time in flight_list:
    dep_time = dep_time[:10]
    flight_dict[f"{dep_code}-{dep_time}"].append(id)
bc2 = sc.broadcast(flight_dict)
flights = bc2.value

def pick_random():
    picked_search = random.choice(searches)
    session_id = picked_search[0]
    searched_ts = picked_search[1]
    user_id = picked_search[2]
    depart_time = picked_search[3].strftime("%Y-%m-%d") # string
    depart_airport_code = picked_search[4]

    return session_id, searched_ts, user_id, depart_time, depart_airport_code

def generate_clicked_ts(searched_ts):
    clicked_ts = searched_ts + timedelta(minutes=random.randint(0, 31))
    return clicked_ts.strftime("%Y-%m-%d %H:%M:%S") # string

def generate_flight_id(depart_time, depart_airport_code):
    now = f"{depart_airport_code}-{depart_time}"
    flight_ids = flights.get(now, [])
    if flight_ids:
        return random.choice(flight_ids)
    else:
        return None

def process_record(record):
    session_id, searched_ts, user_id, depart_time, depart_airport_code = record
    clicked_ts = generate_clicked_ts(searched_ts) # string
    flight_id = generate_flight_id(depart_time, depart_airport_code)
    return session_id, clicked_ts, user_id, flight_id

rdd = sc.parallelize(range(repeat_count), num_partitions)\
    .map(lambda _:pick_random())\
    .map(process_record)

schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("clicked_ts", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("flight_id", StringType(), True)
])

final_df = spark.createDataFrame(rdd, schema)
final_df.write.parquet("s3://hellokorea-extra-data-zone/source/web_log/airline_click_log.parquet/")
spark.stop()