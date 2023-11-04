from dataclasses import dataclass
from datetime import timedelta, time

import pendulum
from pendulum.time import Time
from pendulum.tz.timezone import Timezone

SECS_IN_MIN = 60
MINS_IN_HOUR = 60
HOURS_IN_DAY = 24
DAYS_IN_WEEK = 7

midnight = Time(hour=0, minute=0)
hourly = timedelta(hours=1)
every_4h = timedelta(hours=4)
daily = timedelta(days=1)
weekly = timedelta(days=DAYS_IN_WEEK)

tz_vn: Timezone = Timezone("Asia/Bangkok")
tz_utc: Timezone = pendulum.tz.UTC


def schedule_daily_at(time: time) -> str:
    """Return schedule_interval from given time"""
    return f'{time.minute} {time.hour} * * *'


@dataclass
class DagMetadata:
    dag_id: str
    schedule_time: Time
    schedule_interval: timedelta


# region dim
dim_group = DagMetadata(
    dag_id='dim_group',
    schedule_time=Time(hour=1, minute=0),
    schedule_interval=daily,
)
# endregion dim
