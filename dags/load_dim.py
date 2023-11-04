from datetime import datetime

from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from kubernetes.client.models import V1EnvVar
from dags_info import tz_vn, dim_group, schedule_daily_at
from transform.dim.dim_group import DimGroup
from queries import group


default_args = {
    'owner': 'nhuanbc',
}

dag = DAG(
    dag_id=dim_group.dag_id,
    description='ETL dag for cardano daily',
    default_args=default_args,
    schedule=schedule_daily_at(dim_group.schedule_time),
    start_date=datetime(2023, 2, 12, tzinfo=tz_vn),
    tags=['data_mart', 'dim', 'daily'],
    catchup=False,
)
tz_env = V1EnvVar(name='TZ', value=tz_vn.name)

with TaskGroup(group_id='dim_group', dag=dag) as dim_group:

    build_dim = DimGroup(
        task_id='build_dim_group',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=group['table'],
        sql=group['sql']
    )

