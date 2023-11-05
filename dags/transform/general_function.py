import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from datetime import date
import os
import logging

logger = logging.getLogger(__name__)


class GeneralFunction:

    @classmethod
    def add_date_column(cls, df: pd.DataFrame, execution_date: date) -> pd.DataFrame:
        df['date'] = execution_date
        return df

    @classmethod
    def extract_jira(cls, conn_id: str, sql: str) -> pd.DataFrame:
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        return postgres_hook.get_pandas_df(sql=sql)

    @classmethod
    def load(cls, conn_id: str, table_name: str, df: pd.DataFrame,
             mode: str) -> None:
        get_conn = BaseHook.get_connection(conn_id=conn_id)
        engine = create_engine(
            f'mysql+mysqldb://{get_conn.login}:{get_conn.password}@{get_conn.host}:{get_conn.port}/data_mart'
        )
        df.to_sql(con=engine, name=table_name, if_exists=mode, index=False)

    @classmethod
    def extract_data_mart(cls, conn_id: str, sql: str) -> pd.DataFrame:
        mysql_hook = MySqlHook(mysql_conn_id=conn_id)
        return mysql_hook.get_pandas_df(sql=sql)

    @classmethod
    def extract_excel(cls, file_name):
        file_path = f'{os.getcwd()}/dags/excel_source/{file_name}'
        logger.info(file_path)
        return pd.read_excel(file_path)

    @classmethod
    def tranform_email(cls, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        df[column_name] = df[column_name].str.split('@').str[0].str.lower()
        return df



