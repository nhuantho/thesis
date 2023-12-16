import logging

from airflow.models import BaseOperator
from airflow.utils.context import Context
from dateutil.relativedelta import relativedelta

from transform.general_function import GeneralFunction

logger = logging.getLogger(__name__)


class DimUser(BaseOperator):

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

    def execute(self, context: Context) -> None:
        start_date = context["dag_run"].logical_date.date()
        end_date = start_date + relativedelta(months=1)
        logger.info(f'start_date:{start_date}')
        logger.info(f'end_date:{end_date}')
        sql = self.sql.format(start_date=f'\'{start_date}\'', end_date=f'\'{end_date}\'')
        logger.info(sql)
        extract_data = GeneralFunction.extract_jira(self.conn_jira_id, sql)
        logger.info(extract_data)
        GeneralFunction.load(self.conn_data_mart_id, self.table, extract_data, 'append' if self.append else 'replace')
        logger.info('Done!!!')
