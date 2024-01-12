import logging
from datetime import timedelta

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dateutil.relativedelta import relativedelta
from pandasql import sqldf

from transform.general_function import GeneralFunction

logger = logging.getLogger(__name__)

class FactProductOutputEfficiency(BaseOperator):

    def __init__(
            self,
            task_id: str,
            conn_jira_id: str,
            conn_data_mart_id: str,
            table: str,
            sql: str,
            append: bool = True,
            **kwargs: object
    ) -> None:
        super().__init__(**kwargs, task_id=task_id)
        self.conn_jira_id = conn_jira_id
        self.conn_data_mart_id = conn_data_mart_id
        self.table = table
        self.sql = sql
        self.append = append

    def _tranform_data(
            self, extract_data: pd.DataFrame, done_mm: pd.DataFrame, plan_mm: pd.DataFrame
    ) -> pd.DataFrame:

        return sqldf(
            '''
            select 
                ed.project_id,
                ed.group_id,
                ed.date_id,
                dm.done_mm,
                ed.mm/(pm.plan_mm * (ed.ra/100)) * 100 as EEi,
                dm.done_mm/(pm.plan_mm * (ed.ra/100)) * 100 as EEex
            from extract_data ed
            inner join done_mm dm
                on dm.project_id = ed.project_id and dm.year_month = ed.date_id and dm.group_id = ed.group_id
            inner join plan_mm pm 
                on pm.project_id = ed.project_id and pm.group_id = ed.group_id and pm.time_id = ed.date_id
            '''
        )

    def execute(self, context: Context) -> None:
        start_date = context["dag_run"].logical_date.date() + timedelta(days=1)
        end_date = start_date + relativedelta(months=1)
        logger.info(f'start_date:{start_date}')
        logger.info(f'end_date:{end_date}')
        sql = self.sql.format(start_date=f'\'{start_date}\'', end_date=f'\'{end_date}\'')
        logger.info(sql)
        extract_data = GeneralFunction.extract_jira(self.conn_jira_id, sql)
        done_mm = GeneralFunction.extract_excel('done_mm.xlsx')
        plan_mm = GeneralFunction.extract_excel('plan_mm.xlsx')
        logger.info(extract_data)
        logger.info(done_mm)
        logger.info(plan_mm)
        data_transform = self._tranform_data(extract_data, done_mm, plan_mm)
        logger.info(data_transform)
        GeneralFunction.load(self.conn_data_mart_id, self.table, data_transform, 'append' if self.append else 'replace')
        logger.info('Done!!!')
