from datetime import datetime

from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from kubernetes.client.models import V1EnvVar
from dags_info import tz_vn, build_fact, schedule_daily_at, build_dim
from transform.fact.fact_resource_allowcation import FactResourceAllowcation
from transform.fact.fact_product_output import FactProductOutput
from transform.fact.fact_product_output_efficiency import FactProductOutputEfficiency
from transform.fact.fact_resource_utilization import FactResourceUtilization
from queries import resource_allowcation, product_output, product_output_efficiency, reource_utilization


default_args = {
    'owner': 'nhuanbc',
}

tz_env = V1EnvVar(name='TZ', value=tz_vn.name)

with DAG(
    dag_id=build_fact.dag_id,
    description='ETL dag for fact daily',
    default_args=default_args,
    schedule=schedule_daily_at(build_fact.schedule_time),
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

    build_fact_resource_allocation = FactResourceAllowcation(
        task_id='build_fact_resource_allocation',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=resource_allowcation['table'],
        sql=resource_allowcation['sql'],
        dag=dag
    )

    build_fact_product_output = FactProductOutput(
        task_id='build_fact_product_output',
        conn_jira_id='jira',
        conn_data_mart_id='data_mart',
        table=product_output['table'],
        sql=product_output['sql'],
        dag=dag
    )

    build_fact_product_output_efficiency = FactProductOutputEfficiency(
        task_id='build_fact_product_output_efficiency',
        conn_data_mart_id='data_mart',
        table=product_output_efficiency['table'],
        sql=product_output_efficiency['sql'],
        dag=dag
    )

    build_fact_resource_utilization = FactResourceUtilization(
        task_id='build_fact_resource_utilization',
        conn_data_mart_id='data_mart',
        conn_jira_id='jira',
        sql_data_mart=reource_utilization['sql_data_mart'],
        sql_jira=reource_utilization['sql_jira'],
        table=reource_utilization['table'],
        dag=dag
    )

    build_fact_ra_po = [build_fact_resource_allocation, build_fact_product_output]

    build_fact_ra_po >> build_fact_product_output_efficiency

    build_fact_ra_po >> build_fact_resource_utilization

    build_fact_poe_ru = [build_fact_product_output_efficiency, build_fact_resource_utilization]

    start >> build_fact_ra_po
    build_fact_poe_ru >> end


