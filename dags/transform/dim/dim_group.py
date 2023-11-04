from airflow.models import BaseOperator
from transform.general_function import GeneralFunction
import pandas as pd
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import datetime, date


class DimGroup(BaseOperator):

    INPUT_SCHEMA = DataFrameSchema({
        'id': Column(int),
        'group_name': Column(str),
        'active': Column(int),
        'created_date': Column(datetime),
        'updated_date': Column(datetime)
    })

    OUTPUT_SCHEMA = DataFrameSchema({
        'raw_id': Column(int),
        'group_name': Column(str),
        'active': Column(int),
        'created_date': Column(datetime),
        'updated_date': Column(datetime),
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
                'group_name': 'group_name',
                'active': 'active',
                'created_date': 'created_date',
                'updated_date': 'updated_date',
                'date': 'date'
            })
        )

        return self.OUTPUT_SCHEMA.validate(df_with_date)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        extract_data = GeneralFunction.extract(self.conn_jira_id, self.sql)
        transform_data = self._transform(extract_data, execution_date)
        GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data)




