import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from sqlalchemy.engine import create_engine
from datetime import date


class GeneralFunction:

    @classmethod
    def add_date_column(cls, df: pd.DataFrame, execution_date: date) -> pd.DataFrame:
        df['date'] = execution_date
        return df

    @classmethod
    def extract(cls, conn_id: str, sql: str) -> pd.DataFrame:
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


