from airflow.models import BaseOperator
from transform.general_function import GeneralFunction
import pandas as pd
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import date
import logging

logger = logging.getLogger(__name__)

class DimDate(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'id': Column(int),
        'date': Column(date),
        'day_of_week': Column(int),
        'day_of_month': Column(int),
        'month': Column(int),
        'quarter': Column(int),
        'year': Column(int),
        'is_working_day': Column(int),
        'is_holiday': Column(int),
        'holiday_name': Column(str)
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

    def _transform(self, df: pd.DataFrame, execution_date: date) -> pd.DataFrame:

        df_with_date = (
            GeneralFunction
            .add_date_column(df, execution_date)
            .rename(columns={
                'id': 'raw_id',
                'group_name': 'group_name',
                'active': 'active',
                'created_date': 'created_date',
                'updated_date': 'updated_date',
                'date': 'date'
            })
        )

        df_with_date['raw_id'] = df_with_date['raw_id'].astype(int)
        df_with_date['active'] = df_with_date['active'].astype(int)
        df_with_date['created_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)
        df_with_date['updated_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)

        return self.OUTPUT_SCHEMA.validate(df_with_date)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_group where date=\'{execution_date}\''
        )

        if check_run.empty:
            extract_data = GeneralFunction.extract_jira(self.conn_jira_id, self.sql)
            transform_data = self._transform(extract_data, execution_date)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)




