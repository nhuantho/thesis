import pandas as pd
from pandera import DataFrameSchema, Column
from sqlalchemy.engine import create_engine

# SCHEMA = DataFrameSchema({
#     'id': Column(int),
#     'name': Column(str)
# })
#
# data = {'id': [1, 2], 'name': ['aa', 'bb']}
# df = pd.DataFrame(data=data)
#
#
# def test(df: pd.DataFrame) -> pd.DataFrame:
#     df_tf = df.rename({'id': 'id', 'name': 'name'})
#     return SCHEMA.validate(df_tf)
#
#
# df_test = test(df)
#
# engine = create_engine("mysql+mysqldb://root:nhuan@10.5.0.3/data_mart")
#
# df_test.to_sql(con=engine, name='test', if_exists='append', index=False)

# df = pd.read_excel('excel_source/DS-KSX.xlsx')
#
# df['Email'] = df['Email'].str.split('@').str[0].str.lower()
#
# print(df)

# import holidayapi
# key = '19ad3b47-8e05-4e5b-ae6d-dab8103914da'
# hapi = holidayapi.v1(key)
# holidays = hapi.holidays({
#   'country': 'VN-SG',
#   'year': '2022',
# })
#
# print(holidays)

# import requests
#
# url = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2024'
#
# res = requests.get(
#   url=url,
# )
#
# print(res.json())

df = pd.DataFrame({"date": pd.date_range('2019-01-01', '2030-12-31')})
df['id'] = df['date'].dt.strftime('%Y%m%d').astype(int)
df['day_of_week'] = df['date'].dt.dayofweek.astype(int)
df['day_of_month'] = df['date'].dt.day.astype(int)
df['month'] = df['date'].dt.month.astype(int)
df['quarter'] = df['date'].dt.quarter.astype(int)
df["year"] = df.date.dt.year
df["is_working_day"] = 1
df["is_holiday"] = 0
df["holiday_name"] = ''
print(df)

