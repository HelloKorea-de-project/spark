"""
user_info 테이블에 편향된 국가 정보를 업데이트
"""
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import random, string

# Glue context 추출, Spark session 초기화
sc = SparkContext.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Faker 객체 생성

# get user_id from user_information file
paths = ['s3://hellokorea-extra-data-zone/source/user/user_information.parquet/']
user_id_df = spark.read.parquet(*paths) \
    .select("user_id").sample(False, 0.3)
bc1 = sc.broadcast(user_id_df.collect())
users = bc1.value

# countries : 국가 목록 불러오기
country_df = spark.read.csv("s3://hellokorea-external-zone/source/flight/cheapest_flight_to_ICN_updated_2024-08-01.csv000",
                     header=True, inferSchema=True).select("JP").distinct()
country_collection = country_df.rdd.map(lambda row: row["JP"]).collect()
bc2 = sc.broadcast(country_collection[:len(country_collection) // 2])
countries = bc2.value

# 데이터 행 수 및 분할 개수 설정
TOTAL_DATA_LEN = len(users)
num_partitions = 10
PARTITION_DATA_LEN = TOTAL_DATA_LEN // num_partitions + 1

def generate_country():
    return random.choice(countries)


def process_user(partition_index):
    # get data index appropriate for the partition
    start = partition_index * PARTITION_DATA_LEN
    end = start + PARTITION_DATA_LEN
    if end > TOTAL_DATA_LEN:
        end = TOTAL_DATA_LEN

    results = []
    for row in users[start:end]:
        country = generate_country()
        results.append((
            row.id,
            country
        ))
    return results


processed_rdd = sc.parallelize(range(num_partitions), num_partitions).flatMap(process_user)
final_df = processed_rdd.toDF(['user_id', 'country'])

final_df = final_df.withColumn("user_id", col("user_id").cast(StringType())) \
        .withColumn("country", col("country").cast(StringType()))

final_df.write.parquet("s3://hellokorea-extra-data-zone/source/user/user_information_biased.parquet/")
spark.stop()