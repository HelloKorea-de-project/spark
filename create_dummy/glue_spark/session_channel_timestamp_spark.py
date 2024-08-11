from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql import functions as f
from faker import Faker
import random
from datetime import timedelta


# Glue context 및 Spark session 초기화
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# read airline_search_log file
# input executors = 9, tasks = 9 executors * 8 cores = 72 partitions
log_paths = ['s3://hellokorea-extra-data-zone/source/web_log/airline_search_log.parquet/',
             's3://hellokorea-extra-data-zone/source/web_log/airline_search_log_biased.parquet/']
search_log_df = spark.read.parquet(*log_paths).select('session_id', 'searched_ts') \
                    .groupBy('session_id') \
                        .agg(
                            f.min('searched_ts').alias('earliest_searched_ts'),
                            f.max('searched_ts').alias('latest_searched_ts')
                        )
search_log_list = search_log_df.rdd.collect()
TOTAL_DATA_LEN = len(search_log_list)
NUM_PARTITIONS = 10
PARTITION_DATA_LEN = TOTAL_DATA_LEN // NUM_PARTITIONS + 1

# initiate faker
faker = Faker()

# generating fake data
def generate_partition(partition_index):
    # get data index appropriate for the partition
    start = partition_index * PARTITION_DATA_LEN
    end = start + PARTITION_DATA_LEN
    if end > TOTAL_DATA_LEN:
        end = TOTAL_DATA_LEN
    # generate data
    data = []
    for row in search_log_list[start:end]:
        # get row data from airline search log table
        session_id = row[0]
        earliest_searched_ts = row[1]
        latest_searched_ts = row[2]
        # generate session first created timestamp
        created_ts = generate_timestamp(earliest_searched_ts - timedelta(minutes=30) ,earliest_searched_ts)
        # generate last session connected timestamp
        last_connected_ts = generate_timestamp(latest_searched_ts - timedelta(minutes=30), latest_searched_ts)\
        # generate session started source channel
        channel = generate_channel()
        # append row
        data.append((session_id, created_ts, last_connected_ts, channel))

    return data


def generate_timestamp(start_datetime, end_datetime):
    return faker.date_time_between(start_date=start_datetime, end_date=end_datetime)


def generate_channel():
    return random.choices(
        ['Naver', 'Youtube', 'Instagram', 'Facebook', 'X', 'Google', 'Direct'],
        [0.05, 0.2, 0.15, 0.1, 0.1, 0.3, 0.1]
    )[0]


# RDD 생성
rdd = spark.sparkContext.parallelize(range(NUM_PARTITIONS), NUM_PARTITIONS) \
        .flatMap(generate_partition)

# RDD를 DataFrame으로 변환
df = rdd.toDF(["session_id", "created_ts", "last_connected_ts", "channel"])

# 데이터 타입 변환
df = df.withColumn("session_id", col("session_id").cast(StringType())) \
        .withColumn("created_ts", col("created_ts").cast(TimestampType())) \
        .withColumn("last_connected_ts", col("last_connected_ts").cast(TimestampType())) \
        .withColumn("channel", col("channel").cast(StringType()))

# DataFrame을 S3에 저장
df.write.parquet("s3://hellokorea-extra-data-zone/source/web_log/session_channel_timestamp.parquet")

# Spark session 중지
spark.stop()