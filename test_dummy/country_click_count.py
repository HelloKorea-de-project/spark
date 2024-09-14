from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

user_info = spark.read.parquet('s3://hellokorea-extra-data-zone/source/unloaded/user_info.parquet/')
user_info.createOrReplaceTempView("user_info")

airline_click_log = spark.read.parquet(
    's3://hellokorea-extra-data-zone/source/unloaded/partitioned_airline_click_log.parquet/')
airline_click_log.createOrReplaceTempView("airline_click_log")

# group by 후 조인
result = spark.sql(
    """
    SELECT u.country, SUM(click_count), min(dummy)
        FROM user_info u
        JOIN (SELECT user_id, count(user_id) as click_count
                FROM airline_click_log
                WHERE user_id IS NOT NULL
                GROUP BY user_id) acl
        ON u.user_id = acl.user_id
        GROUP BY country;
    """)

# 조인 후 group by
# result = spark.sql(
#     """
#     SELECT u.country, COUNT(1)
#     FROM airline_click_log acl
#     JOIN user_info u
#     ON u.user_id = acl.user_id
#     GROUP BY 1;
#     """)

result.show()