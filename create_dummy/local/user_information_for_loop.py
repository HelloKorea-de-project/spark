import boto3

import pandas as pd
from io import BytesIO
import random, string

from faker import Faker
from datetime import datetime

def upload_to_s3(idx, all_user_data):
    print(f"task{idx} upload started. {datetime.now}")
    # list to parquet
    col = ['id', 'country', 'pass_num', 'name', 'age', 'gender', 'email', 'registration_date']
    df = pd.DataFrame(data = all_user_data, columns=col)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
    parquet_buffer.seek(0)
    
    s3_key_to_upload = 'your_s3_key'
    s3_bucket_to_upload = 'your_bucket'
    s3_client = boto3.client('s3', aws_access_key_id='your_key', aws_secret_access_key='your_key')
    s3_client.put_object(
            Bucket = s3_bucket_to_upload,
            Key = s3_key_to_upload,
            Body = parquet_buffer.getvalue()
        )
    print(f"task{idx} upload ended. {datetime.now}")

def generate_pass_num(country_code):
    passport_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
    return f"{country_code}-{passport_number}"

def create_user(idx, num_of_users, start_time):
    print(f"task{idx} creating user start")
    user_data = []
    for _ in range(num_of_users):
        id = fake.uuid4()
        country = random.choice(country_list)
        pass_num = generate_pass_num(country)
        name = fake.name()
        age = random.randint(18,90)
        gender = random.choice(['Male','Female'])
        email = fake.email()
        registration_date = fake.date_time_this_year(before_now=True, after_now=False)
        user_data.append([id, country, pass_num, name, age, gender, email, registration_date])
    print(f"task{idx} creating user ended. {datetime.now()-start_time}")
    upload_to_s3(idx, user_data)
    
def generate_task(loop, start_time):
    for idx in range(loop*10, (loop+1)*10):
        create_user(idx, 100000, start_time)

def main():
    start_time = datetime.now()
    for loop in range(10):
        generate_task(loop, start_time)
    print(f"total time : {datetime.now() - start_time}")
    
s3 = boto3.client('s3', aws_access_key_id='your_key', aws_secret_access_key='your_key')
s3_bucket = 'your_bucket'
s3_key = 'your_s3_key'


s3_bucket = 'your_bucket'
s3_key = 'your_s3_key'
obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
result = pd.read_parquet(BytesIO(obj['Body'].read()))
country_list = [result["JP"].unique()]

fake = Faker()
main()