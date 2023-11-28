from datetime import datetime

from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from kubernetes.client.models import V1EnvVar
from dags_info import tz_vn, build_dim, schedule_daily_at
from transform.dim.dim_group import DimGroup
from transform.dim.dim_user import DimUser
from transform.dim.dim_project import DimProject
from queries import group, user, project


default_args = {
    'owner': 'nhuanbc',
}

tz_env = V1EnvVar(name='TZ', value=tz_vn.name)

with DAG(
    dag_id=build_dim.dag_id,
    description='ETL dag for dim daily',
    default_args=default_args,
    schedule=schedule_daily_at(build_dim.schedule_time),
    start_date=datetime(2023, 11, 4, tzinfo=tz_vn),
    tags=['data_mart', 'dim', 'daily'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    build_dim_group = DimGroup(
        task_id='build_dim_group',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=group['table'],
        sql=group['sql'],
        dag=dag
    )

    build_dim_user = DimUser(
        task_id='build_dim_user',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=user['table'],
        sql=user['sql'],
        dag=dag
    )

    build_dim_project = DimProject(
        task_id='build_dim_project',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=project['table'],
        sql=project['sql'],
        dag=dag
    )

    build_dim = [build_dim_group, build_dim_user, build_dim_project]

    start >> build_dim >> end


