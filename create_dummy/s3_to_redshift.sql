-- CREATE session channel timestamp table AND COPY from S3 to redshift
DROP TABLE IF EXISTS raw_data.session_channel_timestamp;
CREATE TABLE raw_data.session_channel_timestamp(
  session_id varchar(256) primary key,
  created_ts timestamp,
  last_connected_ts timestamp,
  channel varchar(32)
);
COPY raw_data.session_channel_timestamp
FROM 's3://hellokorea-extra-data-zone/source/web_log/session_channel_timestamp.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;

-- CREATE user info table and COPY from S3 to redshift
DROP TABLE IF EXISTS raw_data.user_info;
CREATE TABLE raw_data.user_info(
  id varchar(128),
  country varchar(16),
  pass_num varchar(32),
  name varchar(64),
  age bigint,
  gender varchar(16),
  email varchar(64),
  registration_date timestamp
);
COPY raw_data.user_info
FROM 's3://hellokorea-extra-data-zone/source/user/user_information.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;

-- CREATE airline search log table and COPY from S3 to redshift
DROP TABLE IF EXISTS raw_data.airline_search_log;
CREATE TABLE raw_data.airline_search_log(
  session_id varchar(256),
  searched_ts timestamp,
  user_id varchar(256),
  depart_time timestamp,
  depart_airport_code varchar(32),
  arrival_airport_code varchar(32)
);
COPY raw_data.airline_search_log
FROM 's3://hellokorea-extra-data-zone/source/web_log/airline_search_log.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;

-- CREATE airline purchase log table and COPY from S3 to redshift
DROP TABLE IF EXISTS raw_data.airline_purchase_log;
CREATE TABLE raw_data.airline_purchase_log(
  flight_id varchar(128),
  pass_num varchar(32),
  user_id varchar(128),
  purchased_ts TIMESTAMP
);
COPY raw_data.airline_purchase_log
FROM 's3://hellokorea-extra-data-zone/source/web_log/airline_purchase_log.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;