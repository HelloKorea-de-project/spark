import time

import pandas as pd
from faker import Faker
import random
from datetime import datetime

repeat_count = 100000000

faker = Faker()
start = time.time()
print(f"start at: {start}")
session_ids = [str(faker.random_number(digits=32, fix_len=True)) for _ in range(repeat_count)]
ts = [faker.date_time_between(start_date=datetime(2024,1,1), end_date=datetime(2024, 8, 5)) for _ in range(repeat_count)]

channel = [random.choice(['Naver', 'Youtube', 'Instagram', 'Facebook', 'X', 'Google', 'Direct']) for _ in range(repeat_count)]

df = pd.DataFrame()
df['session_id'] = session_ids
df['ts'] = ts
df['channel'] = channel
mid = time.time()
print(f"Dataframe has been created after: {start - mid:.5f} sec")

df.to_parquet('session_channel_timestamp.parquet', index=False)
end = time.time()
print(f"Dataframe has been saved at local directory after: {end - start:.5f} sec")