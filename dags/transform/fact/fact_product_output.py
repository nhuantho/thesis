from airflow.models import BaseOperator
import pandas as pd
from pandasql import sqldf
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import date
import logging
from transform.general_function import GeneralFunction

logger = logging.getLogger(__name__)

class FactProductOutput(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'project_id': Column(int),
        'user_id': Column(int),
        'group_id': Column(int),
        'done_mm': Column(float),
        'complete_date': Column(int),
        'date': Column(date)
    })

    def __init__(
            self,
            task_id: str,
            conn_jira_id: str,
            conn_data_mart_id: str,
            table: str,
            sql: str,
            **kwargs: object
    ) -> None:
        super().__init__(**kwargs, task_id=task_id)
        self.conn_jira_id = conn_jira_id
        self.conn_data_mart_id = conn_data_mart_id
        self.table = table
        self.sql = sql

    def _tranform_data(self, execution_date: date) -> pd.DataFrame:
        product_output = GeneralFunction.extract_jira(self.conn_jira_id, self.sql)
        dim_project = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_project where date=\'{execution_date}\''
        )
        dim_group = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_group where date=\'{execution_date}\''
        )
        dim_user = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_user where date=\'{execution_date}\''
        )
        dim_date = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_date'
        )
        fact_product_output_with_not_date = sqldf(
            '''
            select 
                dp.id as project_id,
                dg.id as group_id,
                du.id as user_id,
                sum(po.story_point) /
                (8*julianday(DATE(po.complete_date, 'start of month', '+1 month', '-1 day')) - julianday(DATE(po.complete_date, 'start of month')))
                as done_mm,
                max(dd.id) as complete_date
            from product_output po
            left join dim_project dp on dp.raw_id = po.project_id
            left join dim_group dg on dg.raw_id = po.group_id
            left join dim_user du on du.raw_id = po.user_id
            left join dim_date dd on dd.date = po.complete_date
            where po.issue_type != 'Sub-task'
                AND po.story_point IS NOT NULL
                AND po.start_date IS NOT NULL
                AND po.end_date IS NOT NULL
                AND po.complete_date IS NOT NULL
                AND po.issue_status != 'Cancelled'
                and dp.id is not null 
                and dg.id is not null 
                and du.id is not null
                and dd.id is not null
            group by dp.id, dg.id, du.id, dd.year_month
            '''
        )

        fact_product_output = (
            GeneralFunction
            .add_date_column(fact_product_output_with_not_date, execution_date)
        )

        fact_product_output['project_id'] = fact_product_output['project_id'].astype(int)
        fact_product_output['group_id'] = fact_product_output['group_id'].astype(int)
        fact_product_output['user_id'] = fact_product_output['user_id'].astype(int)
        fact_product_output['done_mm'] = fact_product_output['done_mm'].astype(float)
        fact_product_output['complete_date'] = fact_product_output['complete_date'].astype(int)

        return self.OUTPUT_SCHEMA.validate(fact_product_output)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from fact_product_output where date=\'{execution_date}\''
        )

        if check_run.empty:
            transform_data = self._tranform_data(execution_date)
            logger.info(transform_data)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)
