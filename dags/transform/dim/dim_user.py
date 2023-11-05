from airflow.models import BaseOperator
from transform.general_function import GeneralFunction
import pandas as pd
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import datetime, date
from pandasql import sqldf
import logging

logger = logging.getLogger(__name__)


class DimUser(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'raw_id': Column(int),
        'user_key': Column(str),
        'display_name': Column(str),
        'lower_display_name': Column(str),
        'email_address': Column(str),
        'role': Column(str, nullable=True),
        'level': Column(str, nullable=True),
        'active': Column(int),
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

    def _transform(self, df: pd.DataFrame, df_level: pd.DataFrame, execution_date: date) -> pd.DataFrame:

        df_level_transform = GeneralFunction.tranform_email(df_level, 'Email')[['Email', 'Role', 'Level']]

        df_tranform_email = (
            GeneralFunction
            .add_date_column(GeneralFunction.tranform_email(df, 'email_address'), execution_date)
        )

        df_merge = sqldf(
            '''
            select 
                dte.id, dte.user_key, dte.display_name, dte.lower_display_name, dte.email_address,
                dlt.Role, dlt.Level, dte.active, dte.created_date, dte.updated_date
            from df_tranform_email dte
            left join df_level_transform dlt 
                on dte.email_address = dlt.Email
            '''
        )

        df_with_date = (
            GeneralFunction
            .add_date_column(df_merge, execution_date)
            .rename(columns={
                'id': 'raw_id',
                'user_key': 'user_key',
                'display_name': 'display_name',
                'lower_display_name': 'lower_display_name',
                'email_address': 'email_address',
                'Role': 'role',
                'Level': 'level',
                'active': 'active',
                'created_date': 'created_date',
                'updated_date': 'updated_date',
                'date': 'date'
            })
        )

        df_with_date['raw_id'] = df_with_date['raw_id'].astype(int)
        df_with_date['active'] = pd.to_numeric(df_with_date['active'], errors='coerce').fillna(0).astype(int)
        df_with_date['created_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)
        df_with_date['updated_date'] = pd.to_datetime(df_with_date['updated_date']).dt.tz_localize(None)

        return self.OUTPUT_SCHEMA.validate(df_with_date)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_user where date=\'{execution_date}\''
        )

        if check_run.empty:
            extract_data_jira = GeneralFunction.extract_jira(self.conn_jira_id, self.sql)
            extract_data_excel = GeneralFunction.extract_excel('DS-KSX.xlsx')
            transform_data = self._transform(extract_data_jira, extract_data_excel, execution_date)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)

