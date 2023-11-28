from airflow.models import BaseOperator
import pandas as pd
from pandasql import sqldf
from airflow.utils.context import Context
from pandera import Column, DataFrameSchema
from datetime import date, timedelta
import logging
from transform.general_function import GeneralFunction
from typing import List, Any
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

class FactResourceUtilization(BaseOperator):

    OUTPUT_SCHEMA = DataFrameSchema({
        'group_id': Column(int),
        'ru': Column(float),
        'date_id': Column(int),
        'date': Column(date)
    })

    def __init__(
            self,
            task_id: str,
            conn_jira_id: str,
            conn_data_mart_id: str,
            table: str,
            sql_data_mart: str,
            sql_jira: str,
            **kwargs: object
    ) -> None:
        super().__init__(**kwargs, task_id=task_id)
        self.conn_jira_id = conn_jira_id
        self.conn_data_mart_id = conn_data_mart_id
        self.table = table
        self.sql_data_mart = sql_data_mart
        self.sql_jira = sql_jira


    def _add_add_date_remove_date(self, data_list: List[Any], execution_date: date) -> List[Any]:
        result = []
        count = 0
        while True:
            if (
                count + 2 <= len(data_list)
                and data_list[count][0] == data_list[count + 1][0]
                and data_list[count][1] == data_list[count + 1][1]
                and data_list[count][2] == 'User added to group'
                and data_list[count + 1][2] == 'User removed from group'
                and data_list[count][3] == data_list[count + 1][3]
            ):
                result.append([
                    data_list[count][0],
                    data_list[count][1],
                    data_list[count][3],
                    data_list[count + 1][3]
                ])
                count = count + 2
                if count == len(data_list):
                    break
            elif (
                count + 2 <= len(data_list)
                and data_list[count][0] == data_list[count + 1][0]
                and data_list[count][1] == data_list[count + 1][1]
                and data_list[count][2] == 'User added to group'
                and data_list[count + 1][2] == 'User removed from group'
            ):
                result.append([
                    data_list[count][0],
                    data_list[count][1],
                    data_list[count][3],
                    data_list[count + 1][3]
                ])
                count = count + 2
                if count == len(data_list):
                    break
            else:
                result.append([
                    data_list[count][0],
                    data_list[count][1],
                    data_list[count][3],
                    execution_date
                ])
                count += 1
            if count + 1 == len(data_list):
                result.append([
                    data_list[count][0],
                    data_list[count][1],
                    data_list[count][3],
                    execution_date
                ])
                break
        return result

    def _data_replication(self, data_list: List[Any]) -> pd.DataFrame:
        result = []
        for data in data_list:
            start_date, end_date = data[2], data[3]
            if start_date.month == end_date.month:
                result.append(data)
            else:
                date_list = pd.date_range(start=start_date, end=end_date, freq='MS')
                if date_list[0].to_pydatetime().date() == start_date:
                    result.append(
                        [data[0], data[1], start_date, date_list[1].to_pydatetime().date() - timedelta(days=1)]
                    )
                else:
                    result.append(
                        [data[0], data[1], start_date, date_list[0].to_pydatetime().date() - timedelta(days=1)]
                    )
                for tmp_date in date_list:
                    tmp_date = tmp_date.to_pydatetime().date()
                    if tmp_date != start_date or tmp_date != end_date:
                        result.append(
                            [data[0], data[1], tmp_date, tmp_date + relativedelta(months=1) - timedelta(days=1)]
                        )

                result.append(
                    [data[0], data[1], date_list[-1].to_pydatetime().date(), end_date]
                )
        return pd.DataFrame(
            result,
            columns=['user_id', 'group_id', 'add_date', 'remove_date']
        )

    def _tranform_data(self, execution_date: date) -> pd.DataFrame:
        resource_allocation = GeneralFunction.extract_data_mart(self.conn_data_mart_id, self.sql_data_mart.format(date=execution_date))
        raw_head_count = (
            self._data_replication(
                self._add_add_date_remove_date(
                    GeneralFunction.extract_jira_to_list(self.conn_jira_id, self.sql_jira), execution_date
                )
            )
        )

        dim_date = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from dim_date'
        )

        raw_head_count['user_id'] = raw_head_count['user_id'].astype(int)
        raw_head_count['group_id'] = raw_head_count['group_id'].astype(int)

        raw_head_count['hc'] = (
                ((raw_head_count['remove_date'] - raw_head_count['add_date']).dt.days + 1) /
                raw_head_count['add_date'].apply(lambda t: pd.Period(t, freq='S').days_in_month)
        )

        tranform_head_count = sqldf(
            '''
            select
                user_id,
                group_id,
                max(hc) as hc,
                min(dd.year_month) as year_month
            from raw_head_count rhc
            inner join dim_date dd on dd.date = rhc.add_date
            group by user_id, group_id, add_date, remove_date
            '''
        )

        fact_resource_utilization_with_not_date = sqldf(
            '''
            select 
                ra.group_id,
                min(ra.date_id) as date_id,
                sum(ra.ra) / sum(thc.hc) as ru
            from resource_allocation ra
            inner join tranform_head_count thc 
            on thc.user_id = ra.raw_user_id
            and thc.group_id = ra.raw_group_id
            and thc.year_month = ra.year_month
            group by ra.group_id, ra.year_month
            '''
        )

        fact_resource_utilization = (
            GeneralFunction
            .add_date_column(fact_resource_utilization_with_not_date, execution_date)
        )

        fact_resource_utilization['group_id'] = fact_resource_utilization['group_id'].astype(int)
        fact_resource_utilization['ru'] = fact_resource_utilization['ru'].astype(float)
        fact_resource_utilization['date_id'] = fact_resource_utilization['date_id'].astype(int)

        return self.OUTPUT_SCHEMA.validate(fact_resource_utilization)

    def execute(self, context: Context) -> None:
        execution_date = context["dag_run"].logical_date.date()
        check_run = GeneralFunction.extract_data_mart(
            self.conn_data_mart_id, f'select * from fact_resource_utilization where date=\'{execution_date}\''
        )

        if check_run.empty:
            transform_data = self._tranform_data(execution_date)
            logger.info(transform_data)
            GeneralFunction.load(self.conn_data_mart_id, self.table, transform_data, 'append')
        else:
            logger.info(check_run)
            logger.info('dag ran today')
            logger.info(execution_date)
