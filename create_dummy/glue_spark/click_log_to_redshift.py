import redshift_connector

conn = redshift_connector.connect(
    host="",
    database="hellokorea_db",
    user="user",
    password="password",
    port=5439
)

conn.autocommit = False
cur = conn.cursor()

try:
    cur.execute("BEGIN;")
    drop_query = "DROP TABLE IF EXISTS raw_data.airline_click_log;"
    cur.execute(drop_query)

    create_table_query = """CREATE TABLE raw_data.airline_click_log(
        session_id VARCHAR(100),
        clicked_ts VARCHAR(100),
        user_id VARCHAR(100),
        flight_id VARCHAR(100)
    );"""
    cur.execute(create_table_query)
    
    copy_query = """
        COPY raw_data.airline_click_log
        FROM 's3://hellokorea-extra-data-zone/source/web_log/airline_click_log.parquet/'
        IAM_ROLE 'arn:aws:iam::862327261051:role/hellokorea_redshift_s3_access_role'
        FORMAT AS PARQUET;"""
    cur.execute(copy_query)
    cur.execute("COMMIT;")
except Exception as e:
    cur.execute("ROLLBACK;")
    raise e

conn.close()