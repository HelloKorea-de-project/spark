"""
Generate searching airline ticket log table

"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, TimestampType, DateType
from faker import Faker
import random
import hashlib
from datetime import datetime, timedelta
import time

# Glue context 및 Spark session 초기화
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 데이터의 행 수 및 분할 개수 설정
repeat_count = 30_000_000
num_partitions = 60

# pick column depTime, depAirportCode from cheapest flight ticket file
csv_path = "s3://hellokorea-external-zone/source/flight/cheapest_flight_to_ICN_updated_2024-08-01.csv000"
airline_ticket_df = spark.read.csv(csv_path, header=True, inferSchema=True) \
                     .select('2024-08-05 09:55:00', 'KIX').sort('KIX')

# broadcast
airline_ticket_list = airline_ticket_df.rdd.collect()
# slice list
airline_ticket_list = airline_ticket_list[:len(airline_ticket_list) // 2]
airline_ticket_broadcast = spark.sparkContext.broadcast(airline_ticket_list)

faker = Faker()

# generating fake data
def generate_session_id():
    rand_digit = str(faker.random_number(digits=10, fix_len=False))
    return hashlib.md5(rand_digit.encode()).hexdigest()

def generate_timestamp():
    return random_date("2024-01-01 00:00:00", "2024-08-04 23:59:59", random.random())


def generate_user_id():
    null_probability = 0.3
    return faker.uuid4() if random.random() > null_probability else None


def pick_airline_ticket_row():
    airline_tickets = airline_ticket_broadcast.value
    picked_ticket = random.choice(airline_tickets)
    depart_time = picked_ticket[0]
    depart_airport_code = picked_ticket[1]
    return depart_time, depart_airport_code, 'ICN'


def generate_row(_):
    session_id = generate_session_id()
    searched_ts = generate_timestamp()
    user_id = generate_user_id()
    depart_date, depart_airport_code, arrival_airport_code = pick_airline_ticket_row()

    return (session_id, searched_ts, user_id, depart_date, depart_airport_code, arrival_airport_code)


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)

# RDD 생성
rdd = spark.sparkContext.parallelize(range(repeat_count), num_partitions) \
    .map(generate_row)

# RDD를 DataFrame으로 변환
df = rdd.toDF(["session_id", "searched_ts", "user_id", "depart_date", "depart_airport_code", "arrival_airport_code"])

# 데이터 타입 변환
df = df.withColumn("session_id", col("session_id").cast(StringType())) \
    .withColumn("searched_ts", col("searched_ts").cast(TimestampType())) \
    .withColumn("user_id", col("user_id").cast(StringType())) \
    .withColumn("depart_date", col("depart_date").cast(DateType())) \
    .withColumn("depart_airport_code", col("depart_airport_code").cast(StringType())) \
    .withColumn("arrival_airport_code", col("arrival_airport_code").cast(StringType())) \


# DataFrame을 S3에 저장
df.write.parquet("s3://hellokorea-extra-data-zone/source/web_log/airline_search_log_biased.parquet")

# Spark session 중지
spark.stop()