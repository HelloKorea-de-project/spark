import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, lit
import random
from datetime import timedelta, datetime
from faker import Faker
import hashlib

# Glue context 추출, Spark session 초기화
sc = SparkContext.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 데이터 행 수 및 파티션 개수 설정
total_count = 400_000_000
num_partitions = 200
partition_len = total_count // num_partitions + 1

user_id = spark.read.parquet('s3://hellokorea-extra-data-zone/source/user/user_information.parquet/').select('id')
top_users_bc = sc.broadcast(user_id.limit(30).collect())
most_users_bc = sc.broadcast(user_id.sample(0.8).collect())

def generate_clicked_ts(faker):
    return faker.date_time_between(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 8, 5))

def generate_session_id(faker):
    rand_digit = str(faker.random_number(digits=10, fix_len=False))
    return hashlib.md5(rand_digit.encode()).hexdigest()


def generate_partition(index, repeat_count, top=True):
    Faker.seed(index)
    faker = Faker()
    partition_len = repeat_count // num_partitions + 1
    if top:
        users = top_users_bc.value
    else:
        users = most_users_bc.value
    data = []
    for _ in range(partition_len):
        data.append(
            (
            random.choice(users),
            generate_session_id(faker),
            generate_clicked_ts(faker)
            )
        )

    return data


rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions) \
    .flatMap(lambda index: generate_partition(index, int(total_count * 0.8)))
df = rdd.toDF(['user_id', 'session_id','clicked_ts'])
df = df.withColumn("user_id", col("user_id").cast(StringType())) \
        .withColumn("session_id", col("session_id").cast(StringType())) \
        .withColumn("clicked_ts", col("clicked_ts").cast(TimestampType()))

rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions) \
    .flatMap(lambda index: generate_partition(index, int(total_count * 0.2), False))

df2 = rdd.toDF(['user_id', 'session_id', 'clicked_ts'])
df2 = df2.withColumn("user_id", col("user_id").cast(StringType())) \
        .withColumn("session_id", col("session_id").cast(StringType())) \
        .withColumn("clicked_ts", col("clicked_ts").cast(TimestampType()))

df = df.union(df2)

df.repartition(400, 'user_id').write.parquet("s3://hellokorea-extra-data-zone/source/web_log/skewed_airline_click_log3.parquet/")
spark.stop()