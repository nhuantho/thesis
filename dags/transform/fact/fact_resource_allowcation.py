from airflow.models import BaseOperator
import pandas as pd
from pandasql import sqldf
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import date, timedelta
import logging
from typing import List, Any
from dateutil.relativedelta import relativedelta
from transform.general_function import GeneralFunction
import numpy as np

logger = logging.getLogger(__name__)

class FactResourceAllowcation(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'raw_id': Column(int, nullable=True),
        'project_id': Column(int, nullable=True),
        'user_id': Column(int, nullable=True),
        'group_id': Column(int, nullable=True),
        'effort_percent': Column(float, nullable=True),
        'ra': Column(float, nullable=True),
        'ra_standard': Column(float, nullable=True),
        'from_date': Column(int, nullable=True),
        'to_date': Column(int, nullable=True),
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

    def _data_replication(self, data_list: List[Any]) -> pd.DataFrame:
        result = []
        for data in data_list:
            start_date, end_date = data[5], data[6]
            if start_date.month == end_date.month:
                result.append(data)
            else:
                date_list = pd.date_range(start=start_date, end=end_date, freq='MS')
                if date_list[0].to_pydatetime().date() == start_date:
                    result.append(
                        [data[0], data[1], data[2], data[3], data[4], start_date,
                         date_list[1].to_pydatetime().date() - timedelta(days=1)]
                    )
                else:
                    result.append(
                        [data[0], data[1], data[2], data[3], data[4], start_date,
                         date_list[0].to_pydatetime().date() - timedelta(days=1)]
                    )
                for tmp_date in date_list:
                    tmp_date = tmp_date.to_pydatetime().date()
                    if tmp_date != start_date or tmp_date != end_date:
                        result.append(
                            [data[0], data[1], data[2], data[3], data[4], tmp_date,
                             tmp_date + relativedelta(months=1) - timedelta(days=1)]
                        )

                result.append(
                    [data[0], data[1], data[2], data[3], data[4], date_list[-1].to_pydatetime().date(), end_date]
                )
        return pd.DataFrame(
            result,
            columns=['raw_id', 'project_id', 'user_id', 'group_id', 'effort_percent', 'from_date', 'to_date']
        )

    def _tranform_data(self, execution_date: date) -> pd.DataFrame:
        data_replication = self._data_replication(
            GeneralFunction.extract_jira_to_list(self.conn_jira_id, self.sql)
        )
        dim_project = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select id, raw_id from dim_project where date=\'{execution_date}\''
        )
        dim_group = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select id, raw_id from dim_group where date=\'{execution_date}\''
        )
        dim_user = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select id, raw_id, level from dim_user where date=\'{execution_date}\''
        )
        dim_date = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select id, date from dim_date'
        )
        data_replication['ra'] = (
                data_replication['effort_percent'] *
                ((data_replication['to_date'] - data_replication['from_date']).dt.days + 1) /
                data_replication['from_date'].apply(lambda t: pd.Period(t, freq='S').days_in_month)
        )
        data_replication['user_id'] = data_replication['user_id'].astype(int)
        data_replication['group_id'] = data_replication['group_id'].astype(int)
        fact_resource_allowcation_with_not_date = sqldf(
            '''
            select 
                dr.raw_id,
                dp.id as project_id,
                dg.id as group_id,
                du.id as user_id,
                effort_percent,
                ra,
                case 
                    when du.level = 'J+' then 1.1
                    when du.level = 'M' then 1.2
                    when du.level = 'M+' then 1.3
                    when du.level = 'S' then 1.4
                    else 1
                end * ra as ra_standard,
                dd_fd.id as from_date,
                dd_td.id as to_date
            from data_replication dr
            left join dim_project dp on dp.raw_id = dr.project_id
            left join dim_group dg on dg.raw_id = dr.group_id
            left join dim_user du on du.raw_id = dr.user_id
            left join dim_date dd_fd on dd_fd.date = dr.from_date
            left join dim_date dd_td on dd_td.date = dr.to_date
            where dp.id is not null 
                and dg.id is not null 
                and du.id is not null
            '''
        )
        logger.info(fact_resource_allowcation_with_not_date[['project_id', 'group_id', 'user_id']])
        fact_resource_allowcation = (
            GeneralFunction
            .add_date_column(fact_resource_allowcation_with_not_date, execution_date)
        )
        fact_resource_allowcation['raw_id'] = fact_resource_allowcation['raw_id'].astype(int)
        fact_resource_allowcation['project_id'] = fact_resource_allowcation['project_id'].astype(int)
        fact_resource_allowcation['group_id'] = fact_resource_allowcation['group_id'].astype(int)
        fact_resource_allowcation['user_id'] = fact_resource_allowcation['user_id'].astype(int)
        fact_resource_allowcation['effort_percent'] = fact_resource_allowcation['effort_percent'].astype(float)
        fact_resource_allowcation['ra'] = fact_resource_allowcation['ra'].astype(float)
        fact_resource_allowcation['ra_standard'] = fact_resource_allowcation['ra_standard'].astype(float)
        fact_resource_allowcation['from_date'] = fact_resource_allowcation['from_date'].astype(int)
        fact_resource_allowcation['to_date'] = fact_resource_allowcation['to_date'].astype(int)

        return self.OUTPUT_SCHEMA.validate(fact_resource_allowcation)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from fact_resource_allowcation where date=\'{execution_date}\''
        )

        if check_run.empty:
            transform_data = self._tranform_data(execution_date)
            logger.info(transform_data)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)
