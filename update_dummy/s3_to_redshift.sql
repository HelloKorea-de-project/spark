-- Update user info table to load country biased data
CREATE TEMP TABLE t(
  id varchar(128),
  country varchar(16)
);
COPY t
FROM 's3://hellokorea-extra-data-zone/source/user/user_information_biased.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;
UPDATE raw_data.user_info
SET country = t.country
FROM t
WHERE raw_data.user_info.user_id = t.id;

-- Update user info table to load age biased data
CREATE TEMP TABLE t(
  id varchar(128),
  age bigint
);
COPY t
FROM 's3://hellokorea-extra-data-zone/source/user/user_information_age_biased.parquet/'
IAM_ROLE 'YOUR-IAM-ROLE'
FORMAT AS PARQUET;
UPDATE raw_data.user_info
SET age = t.age
FROM t
WHERE raw_data.user_info.user_id = t.id;