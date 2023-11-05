from airflow.models import BaseOperator
from transform.general_function import GeneralFunction
import pandas as pd
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import datetime, date
from pandasql import sqldf
import logging

logger = logging.getLogger(__name__)


class DimProject(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'raw_id': Column(int),
        'project_name': Column(str),
        'lead': Column(str),
        'description': Column(str),
        'project_key': Column(str),
        'project_status': Column(int),
        'project_type': Column(str, nullable=True),
        'created_date': Column(datetime, nullable=True),
        'updated_date': Column(datetime, nullable=True),
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

    def _transform(self, df: pd.DataFrame, execution_date: date) -> pd.DataFrame:

        df_with_date = (
            GeneralFunction
            .add_date_column(df, execution_date)
            .rename(columns={
                'id': 'raw_id',
                'project_name': 'project_name',
                'lead': 'lead',
                'description': 'description',
                'project_key': 'project_key',
                'project_status': 'project_status',
                'project_type': 'project_type',
                'create_time': 'created_date',
                'update_time': 'updated_date',
                'date': 'date'
            })
        )

        df_with_date['raw_id'] = df_with_date['raw_id'].astype(int)
        df_with_date['created_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)
        df_with_date['updated_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)

        return self.OUTPUT_SCHEMA.validate(df_with_date)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_project where date=\'{execution_date}\''
        )

        if check_run.empty:
            extract_data_jira = GeneralFunction.extract_jira(self.conn_jira_id, self.sql)
            transform_data = self._transform(extract_data_jira, execution_date)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)

