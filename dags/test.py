import pandas as pd
from pandera import DataFrameSchema, Column
from sqlalchemy.engine import create_engine
from datetime import timedelta, date

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
engine = create_engine("mysql+mysqldb://root:nhuan@10.5.0.3/data_mart")

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

df = pd.DataFrame({"date": pd.date_range('2010-01-01', '2030-12-31')})
df['id'] = df['date'].dt.strftime('%Y%m').astype(str)
# df['day_of_week'] = df['date'].dt.dayofweek.astype(int)
# df['day_of_month'] = df['date'].dt.day.astype(int)
df['month'] = df['date'].dt.month.astype(int)
df['quarter'] = df['date'].dt.quarter.astype(int)
df["year"] = df.date.dt.year.astype(int)
# df["is_working_day"] = 1
# df["is_holiday"] = 0
# df["holiday_name"] = ''
# print(df)
df.drop('date', axis=1, inplace=True)
# print(df)
df.to_sql(con=engine, name='dim_date', if_exists='append', index=False)
# stat_date = date(2021, 1, 2)
# end_date = date(2023, 12, 1)
# d = pd.date_range(stat_date, end_date, freq='MS')
# date_list = []
# date_list.append([date(2021, 1, 11), d[0].to_pydatetime().date() - timedelta(days=1)])
# for i in d:
#     tmp_date = i.to_pydatetime().date()
#     if tmp_date == stat_date or tmp_date == end_date:
#         print('remove')
#     else:
#         print(tmp_date)
#     if tmp_date > date.today():
#         break
#     date_list.append([tmp_date, tmp_date - timedelta(days=1)])
# date_list.append([d[-1].to_pydatetime().date() - timedelta(days=1), date(2023, 12, 15)])
# print(date_list)

# print(d)
