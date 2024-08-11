from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql import functions as f
import time
import random
from datetime import timedelta

# Glue context 및 Spark session 초기화
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# read airline_click_log
paths = ['s3://hellokorea-extra-data-zone/source/web_log/airline_click_log.parquet/',
         's3://hellokorea-extra-data-zone/source/web_log/airline_click_log_biased.parquet/']
log_df = spark.read.parquet(*paths).select('flight_id', 'user_id', 'clicked_ts') \
            .na.drop(subset=['user_id']) \
            .groupBy(['flight_id', 'user_id']) \
            .agg(
                f.max('clicked_ts').alias('last_clicked_ts')
            ) \
            .sample(False, 0.5)

# read user info
info_path = 's3://hellokorea-extra-data-zone/source/user/user_information.parquet/'
info_df = spark.read.parquet(info_path).select('id', 'pass_num') \
            .na.drop(subset=['pass_num'])

# join tables
inner_df = log_df.join(info_df.withColumn('user_id', col('id')), on='user_id')

# shuffle partitions 설정
NUM_PARTITIONS = 36

# generating fake data
def generate_partition(rows):
    data = []
    for row in rows:
        flight_id = row.flight_id
        pass_num = row.pass_num
        user_id = row.user_id
        last_clicked_ts = row.last_clicked_ts
        start_ts = last_clicked_ts.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = (last_clicked_ts + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        purchased_ts = random_date(start_ts, end_ts, random.random())
        data.append((flight_id, pass_num, user_id, purchased_ts))

    return data

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))

def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)

# RDD 생성
rdd = inner_df.repartition(NUM_PARTITIONS).rdd.mapPartitions(generate_partition)

# RDD를 DataFrame으로 변환
df = rdd.toDF(['flight_id', 'pass_num', 'user_id', 'purchased_ts'])

# 데이터 타입 변환
df = df.withColumn("flight_id", col("flight_id").cast(StringType())) \
        .withColumn("pass_num", col("pass_num").cast(StringType())) \
        .withColumn("user_id", col("user_id").cast(StringType())) \
        .withColumn("purchased_ts", col("purchased_ts").cast(TimestampType()))

# DataFrame을 S3에 저장
df.write.parquet("s3://hellokorea-extra-data-zone/source/web_log/airline_purchase_log.parquet")

# Spark session 중지
spark.stop()
