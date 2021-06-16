from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

with DAG(dag_id="mysql_to_mssql", start_date=days_ago(2), tags=['mysql','mssql']) as dag:
    default_args={'owner': 'airflow'},
    schedule_interval=timedelta(days=1),

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    mysql_insert_sql = "INSERT INTO {{ params.table }} ({{ params.col1 }}) VALUES( {{ params.val1 }} )"

    insert_mysql_task = MySqlOperator(
        task_id='insert_data_mysql',
        mysql_conn_id='mysql_local',
        database='test',
        sql=mysql_insert_sql,
        params={
            "table":"airflow_test",
            "col1":"a",
            "val1":"1"
            },
        dag=dag
    )

    with TaskGroup("mysql_create", tooltip="Tasks for mysql_create") as mysql_create:
        mysql_drop_sql = "DROP TABLE IF EXISTS {{ params.table }} ;"

        drop_table_mysql_task = MySqlOperator(
            task_id='drop_table_mysql',
            mysql_conn_id='mysql_local',
            database='test',
            sql=mysql_drop_sql,
            params={"table":"airflow_test"},
            dag=dag
        )

        mysql_create_sql = "CREATE TABLE {{ params.table }} (a int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        create_table_mysql_task = MySqlOperator(
            task_id='create_table_mysql',
            mysql_conn_id='mysql_local',
            database='test',
            sql=mysql_create_sql,
            params={"table":"airflow_test"},
            dag=dag
        )

        drop_table_mysql_task >> create_table_mysql_task


    with TaskGroup("mssql_create", tooltip="Tasks for mssql_create") as mssql_create:
        mssql_drop_sql = "DROP TABLE IF EXISTS {{ params.table }} ;"

        drop_table_mssql_task = MsSqlOperator(
            task_id='drop_table_mssql',
            mssql_conn_id='mssql_local',
            database='test',
            sql=mysql_drop_sql,
            params={"table":"airflow_test"},
            dag=dag
        )

        mssql_create_sql = "CREATE TABLE {{ params.table }} (a int)"

        create_table_mssql_task = MsSqlOperator(
            task_id='create_table_mssql',
            mssql_conn_id='mssql_local',
            database='test',
            sql=mssql_create_sql,
            params={"table":"airflow_test"},
            dag=dag
        )

        drop_table_mssql_task >> create_table_mssql_task

    dest_table = "{{ params.database }}.{{ params.schema }}.{{ params.table }}"
    trans_sql = "select {{ params.col }} from {{ params.database }}.{{ params.table }}"
    
    transform_task = GenericTransfer(
        task_id = "trans_data",
        source_conn_id = "mysql_local",
        destination_conn_id = "mssql_local",
        destination_table = dest_table,
        sql=trans_sql,
        params={
            "database":"test",
            "schema":"dbo",
            "table":"airflow_test",
            "col":"a"
            },
        dag=dag
    )

    start >> mysql_create >> [insert_mysql_task, mssql_create] >> transform_task >> end