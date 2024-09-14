from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

session_channel_ts = spark.read.parquet('s3://hellokorea-extra-data-zone/source/unloaded/session_channel_timestamp.parquet/')
session_channel_ts.createOrReplaceTempView("session_channel_timestamp")

airline_click_log = spark.read.parquet(
    's3://hellokorea-extra-data-zone/source/unloaded/partitioned_with_balance_airline_click_log.parquet/')
airline_click_log.createOrReplaceTempView("airline_click_log")

airline_purchase_log = spark.read.parquet(
    's3://hellokorea-extra-data-zone/source/unloaded/partitioned_with_balance_sorted_airline_purchase_log.parquet/')
airline_purchase_log.createOrReplaceTempView("airline_purchase_log")

result = spark.sql(
    """
    WITH session_user_click_count AS (
        SELECT session_id, user_id, COUNT(1)
        FROM airline_click_log
        GROUP BY 1, 2
    )
    SELECT suc.channel, COUNT(1) as purchase_count
    FROM (SELECT succ.session_id, succ.user_id, sct.channel
            FROM session_user_click_count succ
            JOIN session_channel_timestamp sct
            ON succ.session_id = sct.session_id) suc
    JOIN airline_purchase_log apl
    ON suc.user_id = apl.user_id
    GROUP BY 1;
    """)

result.show()