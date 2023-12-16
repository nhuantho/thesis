from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from kubernetes.client.models import V1EnvVar

from dags_info import tz_vn, build_fact_full
from queries import fact_workforce_productivity_full, fact_product_output_efficiency_full
from transform.fact.fact_product_output_efficiency import FactProductOutputEfficiency
from transform.fact.fact_workforce_productivity import FactWorkforceProductivity

default_args = {
    'owner': 'nhuanbc',
}

tz_env = V1EnvVar(name='TZ', value=tz_vn.name)

with DAG(
    dag_id=build_fact_full.dag_id,
    description='ETL dag for fact daily',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 11, 18, tzinfo=tz_vn),
    tags=['data_mart', 'fact', 'daily'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # sensor = ExternalTaskSensor(
    #     task_id='wait_for_build_dim',
    #     external_dag_id=build_dim.dag_id,
    #     external_task_id='end'
    # )

    fact_workforce_productivity = FactWorkforceProductivity(
        task_id='build_fact_workforce_productivity',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=fact_workforce_productivity_full['table'],
        sql=fact_workforce_productivity_full['sql'],
        append=False,
        dag=dag
    )

    build_fact_product_output_efficiency = FactProductOutputEfficiency(
        task_id='build_fact_product_output_efficiency',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=fact_product_output_efficiency_full['table'],
        sql=fact_product_output_efficiency_full['sql'],
        append=False,
        dag=dag
    )

    build_fact = [build_fact_product_output_efficiency, fact_workforce_productivity]

    start >> build_fact >> end


