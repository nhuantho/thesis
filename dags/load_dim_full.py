from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from kubernetes.client.models import V1EnvVar
from dags_info import tz_vn, build_dim_full, schedule_monthly_at
from transform.dim.dim_group import DimGroup
from transform.dim.dim_user import DimUser
from transform.dim.dim_project import DimProject
from queries import group_full, user_full, project_full


default_args = {
    'owner': 'nhuanbc',
}

tz_env = V1EnvVar(name='TZ', value=tz_vn.name)

with DAG(
    dag_id=build_dim_full.dag_id,
    description='ETL dag for dim daily',
    default_args=default_args,
    schedule=None,
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
        table=group_full['table'],
        sql=group_full['sql'],
        append=False,
        dag=dag
    )

    build_dim_user = DimUser(
        task_id='build_dim_user',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=user_full['table'],
        sql=user_full['sql'],
        append=False,
        dag=dag
    )

    build_dim_project = DimProject(
        task_id='build_dim_project',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=project_full['table'],
        sql=project_full['sql'],
        append=False,
        dag=dag
    )

    build_dim = [build_dim_group, build_dim_user, build_dim_project]

    start >> build_dim >> end


