from airflow.models import BaseOperator
import pandas as pd
from pandasql import sqldf
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import date
import logging
from transform.general_function import GeneralFunction

logger = logging.getLogger(__name__)

class FactProductOutputEfficiency(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'project_id': Column(int),
        'group_id': Column(int),
        'EEi': Column(float),
        'EEex': Column(float),
        'EEtc': Column(float),
        'date_id': Column(int),
        'date': Column(date)
    })

    def __init__(
            self,
            task_id: str,
            conn_data_mart_id: str,
            table: str,
            sql: str,
            **kwargs: object
    ) -> None:
        super().__init__(**kwargs, task_id=task_id)
        self.conn_data_mart_id = conn_data_mart_id
        self.table = table
        self.sql = sql

    def _tranform_data(self, execution_date: date) -> pd.DataFrame:
        product_output = GeneralFunction.extract_data_mart(self.conn_data_mart_id, self.sql.format(date=execution_date))
        product_output_acceptance = GeneralFunction.extract_excel('import_sanluong.xlsx')
        dim_date = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_date'
        )

        fact_product_output_efficiency_with_not_date = sqldf(
            '''
            select 
                po.project_id,
                po.group_id,
                po.EEi,
                poa.mm / po.ra as EEex,
                poa.mm / po.ra_standard as EEtc,
                po.date_id
            from product_output po
            inner join dim_date dd on dd.id = po.date_id
            inner join product_output_acceptance poa 
                on upper(poa.pname) = upper(po.project_name) 
                and upper(poa.bu) = upper(po.group_name)
                and dd.month = cast(poa.month as integer)
                and dd.year = cast(poa.year as integer)
            '''
        )

        fact_product_output_efficiency = (
            GeneralFunction
            .add_date_column(fact_product_output_efficiency_with_not_date, execution_date)
        )

        fact_product_output_efficiency['project_id'] = fact_product_output_efficiency['project_id'].astype(int)
        fact_product_output_efficiency['group_id'] = fact_product_output_efficiency['group_id'].astype(int)
        fact_product_output_efficiency['EEi'] = fact_product_output_efficiency['EEi'].astype(float)
        fact_product_output_efficiency['EEex'] = fact_product_output_efficiency['EEex'].astype(float)
        fact_product_output_efficiency['EEtc'] = fact_product_output_efficiency['EEtc'].astype(float)
        fact_product_output_efficiency['date_id'] = fact_product_output_efficiency['date_id'].astype(int)

        return self.OUTPUT_SCHEMA.validate(fact_product_output_efficiency)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from fact_product_output_efficiency where date=\'{execution_date}\''
        )

        if check_run.empty:
            transform_data = self._tranform_data(execution_date)
            logger.info(transform_data)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)
