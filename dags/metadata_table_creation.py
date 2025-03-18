from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import os

_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_DB = "DEMO_DB"
_SNOWFLAKE_SCHEMA = "DEMO_SCHEMA"
_SNOWFLAKE_TABLE = "DEMO_TABLE"


@dag(
    dag_display_name="METADATA TABLE CREATION DAG ❄️",
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
def metadata_table_creation():

    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    create_or_replace_table = SQLExecuteQueryOperator(
        task_id="create_or_replace_table",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="DEMO_DB",
        sql=f"""
            CALL DEMO_DB.DEMO_SCHEMA.CREATE_TABLES_FROM_METADATA()
        """,
    )

    chain(
        create_or_replace_table
    )


metadata_table_creation()