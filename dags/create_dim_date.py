import pandas

data = {
    'meta': {
        'code': 200
    }, 
    'response': {
        'holidays': [
            {
                'name': "International New Year's Day", 
                'description': 'New Year’s Day is the first day of the year, or January 1, in the Gregorian calendar.', 
                'country': {
                    'id': 'vn', 
                    'name': 'Vietnam'
                }, 
                'date': {
                    'iso': '2023-01-01', 
                    'datetime': {
                        'year': 2023, 
                        'month': 1, 
                        'day': 1
                    }
                }, 
                'type': ['National holiday'], 
                'primary_type': 'National holiday', 
                'canonical_url': 'https://calendarific.com/holiday/vietnam/new-year-day', 
                'urlid': 'vietnam/new-year-day', 
                'locations': 'All', 
                'states': 'All'
            }, 
            {
                'name': "Day off for International New Year's Day", 
                'description': 'New Year’s Day is the first day of the year, or January 1, in the Gregorian calendar.', 
                'country': {
                    'id': 'vn', 
                    'name': 'Vietnam'
                }, 
                'date': {
                    'iso': '2023-01-02', 
                    'datetime': {
                        'year': 2023, 
                        'month': 1, 
                        'day': 2
                    }
                }, 
                'type': ['National holiday'], 
                'primary_type': 'National holiday', 
                'canonical_url': 
                'https://calendarific.com/holiday/vietnam/new-year-day', 
                'urlid': 'vietnam/new-year-day', 
                'locations': 'All', 
                'states': 'All'
            }, 
            {
                'name': 'Tet Holiday', 'description': 'Tet Holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-20', 'datetime': {'year': 2023, 'month': 1, 'day': 20}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-holiday', 'urlid': 'vietnam/tet-holiday', 'locations': 'All', 'states': 'All'}, {'name': "Vietnamese New Year's Eve", 'description': 'Lunar New Year is the first day of the Chinese calendar, which is a lunisolar calendar mainly used for traditional celebrations.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-21', 'datetime': {'year': 2023, 'month': 1, 'day': 21}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/spring-festival-golden-week', 'urlid': 'vietnam/spring-festival-golden-week', 'locations': 'All', 'states': 'All'}, {'name': 'Vietnamese New Year', 'description': 'Vietnamese New Year is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-22', 'datetime': {'year': 2023, 'month': 1, 'day': 22}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-new-year', 'urlid': 'vietnam/tet-new-year', 'locations': 'All', 'states': 'All'}, {'name': 'Tet holiday', 'description': 'Tet holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-23', 'datetime': {'year': 2023, 'month': 1, 'day': 23}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-holiday-1', 'urlid': 'vietnam/tet-holiday-1', 'locations': 'All', 'states': 'All'}, {'name': 'Tet holiday', 'description': 'Tet holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-24', 'datetime': {'year': 2023, 'month': 1, 'day': 24}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-holiday-2', 'urlid': 'vietnam/tet-holiday-2', 'locations': 'All', 'states': 'All'}, {'name': 'Tet holiday', 'description': 'Tet holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-25', 'datetime': {'year': 2023, 'month': 1, 'day': 25}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-holiday-3', 'urlid': 'vietnam/tet-holiday-3', 'locations': 'All', 'states': 'All'}, {'name': 'Tet holiday', 'description': 'Tet holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-01-26', 'datetime': {'year': 2023, 'month': 1, 'day': 26}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/tet-holiday-4', 'urlid': 'vietnam/tet-holiday-4', 'locations': 'All', 'states': 'All'}, {'name': "Valentine's Day", 'description': "February 14 is Valentine's Day or Saint Valentine's Feast. The day of love owes its origins to ancient Roman and European Christian traditions.", 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-02-14', 'datetime': {'year': 2023, 'month': 2, 'day': 14}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/valentine-day', 'urlid': 'vietnam/valentine-day', 'locations': 'All', 'states': 'All'}, {'name': 'March Equinox', 'description': 'March Equinox in Vietnam (Hanoi)', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-03-21T04:24:20+07:00', 'datetime': {'year': 2023, 'month': 3, 'day': 21, 'hour': 4, 'minute': 24, 'second': 20}, 'timezone': {'offset': '+07:00', 'zoneabb': 'ICT', 'zoneoffset': 25200, 'zonedst': 0, 'zonetotaloffset': 25200}}, 'type': ['Season'], 'primary_type': 'Season', 'canonical_url': 'https://calendarific.com/holiday/seasons/vernal-equinox', 'urlid': 'seasons/vernal-equinox', 'locations': 'All', 'states': 'All'}, {'name': 'Easter Sunday', 'description': 'Easter Sunday commemorates Jesus Christ’s resurrection, according to Christian belief.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-04-09', 'datetime': {'year': 2023, 'month': 4, 'day': 9}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/easter-sunday', 'urlid': 'vietnam/easter-sunday', 'locations': 'All', 'states': 'All'}, {'name': 'Hung Kings Festival', 'description': 'Hung Kings Festival is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-04-29', 'datetime': {'year': 2023, 'month': 4, 'day': 29}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/vietnamese-kings-day', 'urlid': 'vietnam/vietnamese-kings-day', 'locations': 'All', 'states': 'All'}, {'name': 'Liberation Day/Reunification Day', 'description': 'Liberation Day/Reunification Day is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-04-30', 'datetime': {'year': 2023, 'month': 4, 'day': 30}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/liberation-day', 'urlid': 'vietnam/liberation-day', 'locations': 'All', 'states': 'All'}, {'name': 'International Labor Day', 'description': "Labor Day, International Workers' Day, and May Day, is a day off for workers in many countries around the world.", 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-05-01', 'datetime': {'year': 2023, 'month': 5, 'day': 1}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/labor-day', 'urlid': 'vietnam/labor-day', 'locations': 'All', 'states': 'All'}, {'name': 'Day off for Hung Kings Festival', 'description': 'Hung Kings Festival is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-05-02', 'datetime': {'year': 2023, 'month': 5, 'day': 2}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/vietnamese-kings-day', 'urlid': 'vietnam/vietnamese-kings-day', 'locations': 'All', 'states': 'All'}, {'name': 'Day off for Liberation Day/Reunification Day', 'description': 'Liberation Day/Reunification Day is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-05-03', 'datetime': {'year': 2023, 'month': 5, 'day': 3}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/liberation-day', 'urlid': 'vietnam/liberation-day', 'locations': 'All', 'states': 'All'}, {'name': 'Vesak', 'description': 'Vesak is a observance in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-05-05', 'datetime': {'year': 2023, 'month': 5, 'day': 5}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/vesak', 'urlid': 'vietnam/vesak', 'locations': 'All', 'states': 'All'}, {'name': 'June Solstice', 'description': 'June Solstice in Vietnam (Hanoi)', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-06-21T21:57:49+07:00', 'datetime': {'year': 2023, 'month': 6, 'day': 21, 'hour': 21, 'minute': 57, 'second': 49}, 'timezone': {'offset': '+07:00', 'zoneabb': 'ICT', 'zoneoffset': 25200, 'zonedst': 0, 'zonetotaloffset': 25200}}, 'type': ['Season'], 'primary_type': 'Season', 'canonical_url': 'https://calendarific.com/holiday/seasons/june-solstice', 'urlid': 'seasons/june-solstice', 'locations': 'All', 'states': 'All'}, {'name': 'Vietnamese Family Day', 'description': 'Vietnamese Family Day is a observance in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-06-28', 'datetime': {'year': 2023, 'month': 6, 'day': 28}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/family-day', 'urlid': 'vietnam/family-day', 'locations': 'All', 'states': 'All'}, {'name': 'Independence Day Holiday', 'description': 'Independence Day Holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-09-01', 'datetime': {'year': 2023, 'month': 9, 'day': 1}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/independence-day-holiday-1', 'urlid': 'vietnam/independence-day-holiday-1', 'locations': 'All', 'states': 'All'}, {'name': 'Independence Day', 'description': 'Independence Day is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-09-02', 'datetime': {'year': 2023, 'month': 9, 'day': 2}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/independence-day', 'urlid': 'vietnam/independence-day', 'locations': 'All', 'states': 'All'}, {'name': 'Independence Day Holiday', 'description': 'Independence Day Holiday is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-09-03', 'datetime': {'year': 2023, 'month': 9, 'day': 3}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/independence-day-holiday-2', 'urlid': 'vietnam/independence-day-holiday-2', 'locations': 'All', 'states': 'All'}, {'name': 'Independence Day observed', 'description': 'Independence Day is a national holiday in Vietnam', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-09-04', 'datetime': {'year': 2023, 'month': 9, 'day': 4}}, 'type': ['National holiday'], 'primary_type': 'National holiday', 'canonical_url': 'https://calendarific.com/holiday/vietnam/independence-day', 'urlid': 'vietnam/independence-day', 'locations': 'All', 'states': 'All'}, {'name': 'September Equinox', 'description': 'September Equinox in Vietnam (Hanoi)', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-09-23T13:50:01+07:00', 'datetime': {'year': 2023, 'month': 9, 'day': 23, 'hour': 13, 'minute': 50, 'second': 1}, 'timezone': {'offset': '+07:00', 'zoneabb': 'ICT', 'zoneoffset': 25200, 'zonedst': 0, 'zonetotaloffset': 25200}}, 'type': ['Season'], 'primary_type': 'Season', 'canonical_url': 'https://calendarific.com/holiday/seasons/autumnal-equniox', 'urlid': 'seasons/autumnal-equniox', 'locations': 'All', 'states': 'All'}, {'name': "Vietnamese Women's Day", 'description': "Vietnamese Women's Day is a observance in Vietnam", 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-10-20', 'datetime': {'year': 2023, 'month': 10, 'day': 20}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/vietnamese-women-day', 'urlid': 'vietnam/vietnamese-women-day', 'locations': 'All', 'states': 'All'}, {'name': 'Halloween', 'description': 'Halloween is a festive occasion that is celebrated in many countries on October 31 each year.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-10-31', 'datetime': {'year': 2023, 'month': 10, 'day': 31}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/halloween', 'urlid': 'vietnam/halloween', 'locations': 'All', 'states': 'All'}, {'name': 'December Solstice', 'description': 'December Solstice in Vietnam (Hanoi)', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-12-22T10:27:21+07:00', 'datetime': {'year': 2023, 'month': 12, 'day': 22, 'hour': 10, 'minute': 27, 'second': 21}, 'timezone': {'offset': '+07:00', 'zoneabb': 'ICT', 'zoneoffset': 25200, 'zonedst': 0, 'zonetotaloffset': 25200}}, 'type': ['Season'], 'primary_type': 'Season', 'canonical_url': 'https://calendarific.com/holiday/seasons/december-solstice', 'urlid': 'seasons/december-solstice', 'locations': 'All', 'states': 'All'}, {'name': 'Christmas Eve', 'description': 'Christmas Eve is the day before Christmas Day and falls on December 24 in the Gregorian calendar.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-12-24', 'datetime': {'year': 2023, 'month': 12, 'day': 24}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/christmas-eve', 'urlid': 'vietnam/christmas-eve', 'locations': 'All', 'states': 'All'}, {'name': 'Christmas Day', 'description': 'Christmas Day is one of the biggest Christian celebrations and falls on December 25 in the Gregorian calendar.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-12-25', 'datetime': {'year': 2023, 'month': 12, 'day': 25}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/christmas-day', 'urlid': 'vietnam/christmas-day', 'locations': 'All', 'states': 'All'}, {'name': "International New Year's Eve", 'description': 'New Year’s Eve is the last day of the year, December 31, in the Gregorian calendar.', 'country': {'id': 'vn', 'name': 'Vietnam'}, 'date': {'iso': '2023-12-31', 'datetime': {'year': 2023, 'month': 12, 'day': 31}}, 'type': ['Observance'], 'primary_type': 'Observance', 'canonical_url': 'https://calendarific.com/holiday/vietnam/new-year-eve', 'urlid': 'vietnam/new-year-eve', 'locations': 'All', 'states': 'All'}]}}
data_api = []
for i in data['response']['holidays']:
    data_api.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )

url = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2024'

import requests
import pandas as pd
from sqlalchemy.engine import create_engine
from pandasql import sqldf
import time

engine = create_engine(
    f'mysql+mysqldb://root:nhuan@10.5.0.3:3306/data_mart'
)

url1 = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2019'
url2 = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2020'
url3 = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2021'
url4 = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2022'
url5 = 'https://calendarific.com/api/v2/holidays?&api_key=XDZGugycOGr7kcly6uha4o5QfSpuRaME&country=vn&year=2023'
res1 = requests.get(
  url=url1,
)
time.sleep(1)
res2 = requests.get(
  url=url2,
)
time.sleep(1)
res3 = requests.get(
  url=url3,
)
time.sleep(1)
res4 = requests.get(
  url=url4,
)
time.sleep(1)
res5 = requests.get(
  url=url5,
)

data_holodays = []

for i in res1.json()['response']['holidays']:
    data_holodays.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )

for i in res2.json()['response']['holidays']:
    data_holodays.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )

for i in res3.json()['response']['holidays']:
    data_holodays.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )

for i in res4.json()['response']['holidays']:
    data_holodays.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )

for i in res5.json()['response']['holidays']:
    data_holodays.append(
        [i['name'], i['date']['datetime']['year'], i['date']['datetime']['month'], i['date']['datetime']['day']]
    )


df = pd.DataFrame({"date": pd.date_range('2019-01-01', '2100-12-31')})
df['id'] = df['date'].dt.strftime('%Y%m%d').astype(int)
df['day_of_week'] = df['date'].dt.dayofweek.astype(int)
df['day_of_month'] = df['date'].dt.day.astype(int)
df['month'] = df['date'].dt.month.astype(int)
df['quarter'] = df['date'].dt.quarter.astype(int)
df["year"] = df.date.dt.year
df['year_month'] = df['date'].dt.strftime('%Y%m').astype(int)
df["is_working_day"] = 1
df["is_holiday"] = 0
df["holiday_name"] = ''
print(df)

df_api = pd.DataFrame(data_holodays, columns=['holiday_name', 'year', 'month', 'day'])
df_api['is_holiday'] = 1
df_api['date'] = pandas.to_datetime(df_api[['year', 'month', 'day']])

df_merge = sqldf(
    '''
    select 
        d.id,
        d.date,
        d.day_of_week,
        d.day_of_month,
        d.month,
        d.quarter,
        d.year,
        d.year_month,
        case 
            when da.date is null then d.is_working_day
            else 0
        end as is_working_day,
        case 
            when da.date is null then d.is_holiday
            else 1
        end as is_holiday, 
        case 
            when da.date is null then d.holiday_name
            else da.holiday_name
        end as holiday_name
    from df d 
    left join df_api da 
        on da.date = d.date
    '''
)

df_group_by = sqldf(
    '''
    select 
        d.id,
        max(d.date) as date,
        max(d.day_of_week) as day_of_week,
        max(d.day_of_month) as day_of_month,
        max(d.month) as month,
        max(d.quarter) as quarter,
        max(d.year) as year,
        max(d.year_month) year_month,
        max(d.is_working_day) is_working_day,
        max(d.is_holiday) is_holiday, 
        case 
            when d.holiday_name != '' then d.holiday_name || ',' 
            else '' 
        end as holiday_name
    from df_merge d
    group by d.id
    '''
)

df_group_by.to_sql(con=engine, name='dim_date', if_exists='append', index=False)

print(
    sqldf(
        '''
        select *
        from df_merge
        where is_holiday = 1
        '''
    )
)
