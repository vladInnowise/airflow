from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import snowflake_models, sql_query


path_to_file = Variable.set('load_file', '/Users/vladislav/PycharmProjects/airflow/snowflakefolder/763K_plus_IOS_Apps_Info.csv')

default_args = {
    'start_date': datetime(2022, 7, 25, 1, 1),
    'schedule_interval': "@daily"
}


with DAG('snowflake', default_args=default_args) as dag:
    create_tables_and_streams = SnowflakeOperator(
        task_id='create_db_and_t',
        snowflake_conn_id='snowflake_good',
        sql=f"{sql_query.query_1}"
    )

    insert_data = PythonOperator(
    task_id='insert_pd_to_snowflake',
    python_callable=snowflake_models.load_data,
    dag=dag
    )

    insert_from_raw_stream = SnowflakeOperator(
        task_id='insert_from_raw_stream',
        snowflake_conn_id='snowflake_good',
        sql=
        f'insert into stage_table select {sql_query.columns} from raw_stream;'
    )


    insert_from_stage_stream = SnowflakeOperator(
        task_id='insert_from_stage_stream',
        snowflake_conn_id='snowflake_good',
        sql=f'insert into master_table select {sql_query.columns} from stage_stream;'
    )


    create_tables_and_streams >> insert_data >> insert_from_raw_stream >> insert_from_stage_stream
