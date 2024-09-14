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
    's3://hellokorea-extra-data-zone/source/web_log/skewed_airline_click_log3.parquet/')
airline_click_log.createOrReplaceTempView("airline_click_log")

# group by 후 조인
result = spark.sql(
    """
    SELECT
	u.age - MOD(u.age, 10) AS age_group,
	SUM(click_count)
    FROM user_info u
    JOIN (
        SELECT user_id, COUNT(user_id) as click_count
        FROM airline_click_log
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    ) ucc
        ON u.user_id = ucc.user_id
    GROUP BY age_group;
    """)

# result = spark.sql(
#     """
#     SELECT age - MOD(age, 10) as age_group, COUNT(1)
#     FROM airline_click_log acl
#     JOIN user_info u
#     ON acl.user_id = u.user_id
#     GROUP BY 1
#     """
# )

result.show()