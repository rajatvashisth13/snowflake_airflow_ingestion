from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import os

_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_DB = "DEMO_DB"
_SNOWFLAKE_SCHEMA = "DEMO_SCHEMA"
_SNOWFLAKE_TABLE = "EMPLOYEE"
_SNOWFLAKE_STAGE = "DEMO_STAGE"


@dag(
    dag_display_name="COPY INTO DAG ❄️",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["tutorial"],
    template_searchpath=[
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/sql")
    ],  # path to the SQL templates
)
def copyinto_dag():

    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    copy_into_table = CopyFromExternalStageToSnowflakeOperator(
                task_id=f"copy_into_{_SNOWFLAKE_TABLE}_table",
                snowflake_conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB,
                schema=_SNOWFLAKE_SCHEMA,
                table=_SNOWFLAKE_TABLE,
                stage=_SNOWFLAKE_STAGE,
                pattern=".*[.]csv",
                file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
            )

    chain(
        copy_into_table
    )


copyinto_dag()