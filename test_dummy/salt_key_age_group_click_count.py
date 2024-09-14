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


salt_size = 5
arr = ', '.join([str(i) for i in range(salt_size)])

result = spark.sql(
    f"""
    SELECT age - MOD(age, 10) as age_group, COUNT(1)
	FROM (
	    SELECT user_id, FLOOR(RAND() * {salt_size}) AS rnd
	    FROM airline_click_log
	)
	airline_click_log INNER JOIN (
	    SELECT user_id, age, EXPLODE(ARRAY({arr})) AS rnd
	    FROM user_info
	)
	user_info USING(user_id, rnd)
    GROUP BY age_group;
    """)


result.show()