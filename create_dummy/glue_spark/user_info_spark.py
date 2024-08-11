from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from faker import Faker
import random, string
from datetime import datetime, timedelta

# Glue context 추출, Spark session 초기화
sc = SparkContext.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# 검색 기록에서 unique id 가져오기
log_paths = ['s3://hellokorea-extra-data-zone/source/web_log/airline_search_log.parquet/',
             's3://hellokorea-extra-data-zone/source/web_log/airline_search_log_biased.parquet/']
user_id_df = spark.read.parquet(*log_paths) \
    .select("user_id").distinct().na.drop(subset=['user_id'])
bc1 = sc.broadcast(user_id_df.collect())
users = bc1.value

# countries : 국가 목록 불러오기
country_df = spark.read.csv(
    "s3://hellokorea-external-zone/source/flight/cheapest_flight_to_ICN_updated_2024-08-01.csv000",
    header=True, inferSchema=True).select("JP").distinct()
bc2 = sc.broadcast(country_df.rdd.map(lambda row: row["JP"]).collect())
countries = bc2.value

# 데이터 행 수 및 분할 개수 설정
TOTAL_DATA_LEN = len(users)
num_partitions = 20
PARTITION_DATA_LEN = TOTAL_DATA_LEN // num_partitions + 1


def generate_country():
    return random.choice(countries)


def generate_pass_number(country):
    number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
    null_probability = 0.3
    return f"{country}-{number}" if random.random() > null_probability else None


def generate_name(fake):
    return fake.name()


def generate_age():
    return random.randint(18, 100)


def generate_gender():
    return random.choice(['Male', 'Female'])


def generate_email(fake):
    return fake.email()


def generate_registration_date(fake):
    return fake.date_time_between(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31))


def process_user(partition_index):
    faker = Faker()
    # get data index appropriate for the partition
    start = partition_index * PARTITION_DATA_LEN
    end = start + PARTITION_DATA_LEN
    if end > TOTAL_DATA_LEN:
        end = TOTAL_DATA_LEN

    results = []
    for row in users[start:end]:
        country = generate_country()
        results.append((
            row.user_id,
            country,
            generate_pass_number(country),
            generate_name(faker),
            generate_age(),
            generate_gender(),
            generate_email(faker),
            generate_registration_date(faker)
        ))
    return results


processed_rdd = sc.parallelize(range(num_partitions), num_partitions).flatMap(process_user)
final_df = processed_rdd.toDF(['user_id', 'country', 'pass_num', 'name', 'age', 'gender', 'email', 'registration_date'])

final_df = final_df.withColumn("user_id", col("user_id").cast(StringType())) \
    .withColumn("country", col("country").cast(StringType())) \
    .withColumn("pass_num", col("pass_num").cast(StringType())) \
    .withColumn("name", col("name").cast(StringType())) \
    .withColumn("age", col("age").cast(LongType())) \
    .withColumn("gender", col("gender").cast(StringType())) \
    .withColumn("email", col("email").cast(StringType())) \
    .withColumn("registration_date", col("registration_date").cast(TimestampType()))

final_df.write.parquet("s3://hellokorea-extra-data-zone/source/user/user_information.parquet/")
spark.stop()
