from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import IntegerType, ArrayType, StringType

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

user_info = spark.read.parquet('s3://hellokorea-extra-data-zone/source/unloaded/user_info.parquet/')

airline_click_log = spark.read.parquet(
    's3://hellokorea-extra-data-zone/source/web_log/skewed_airline_click_log3.parquet/')

PARTITION_SIZE = 1_000_000 # 100_000 >> 10_000 >> 1000 >> 100(data skew solved)

clicked_user_counts = airline_click_log.groupBy('user_id').count().filter(col('count') > PARTITION_SIZE)


@udf(returnType=IntegerType())
def get_salting_size(count):
    if count is None:
        return 1
    div = count // PARTITION_SIZE
    remain = count % PARTITION_SIZE

    if remain == 0:
        return div

    return div + 1


@udf(returnType=ArrayType(IntegerType()))
def get_salting_array(size):
    return list(range(size))


# airline click log
airline_click_log = airline_click_log.join(broadcast(clicked_user_counts), on='user_id', how='left')
airline_click_log = airline_click_log.withColumn("salting_size", get_salting_size(col('count')))
airline_click_log = airline_click_log.withColumn(
    "salting_key", (rand() * col('salting_size')).cast(IntegerType())
)
airline_click_log.createOrReplaceTempView('airline_click_log')

# user info
user_info = user_info.join(broadcast(clicked_user_counts), on='user_id', how='left')
user_info = user_info.withColumn('salting_size', get_salting_size(col('count')))
user_info = user_info.withColumn(
    'salting_key', explode(get_salting_array(col("salting_size")))
)
user_info.createOrReplaceTempView('user_info')

# result
# result = airline_click_log.join(user_info, on=['user_id', 'salting_key'], how='left')
result = spark.sql("""
    SELECT age - MOD(age, 10) as age_group, COUNT(1)
    FROM airline_click_log acl
    JOIN user_info u
    ON acl.user_id = u.user_id and acl.salting_key = u.salting_key
    GROUP BY 1
""")

result.show()