from datetime import datetime, timedelta
import os
from airflow import conf
from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook
from airflow.operators.python_operator import PythonOperator

from plugins.helpers import SqlQueries

# Default args 
default_args = {
    'owner': 'vaibhav-bansal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
)
def airflow_project():

    @task()
    def read_sql_file():
        with open(os.path.join(conf.get('core', 'dags_folder'), 'create_tables.sql'), 'r') as f:
            return f.read()

    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=read_sql_file()
    )

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        copy_options=['FORMAT AS JSON \'s3://udacity-dend/log_json_path.json\'']
    )

    stage_songs_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        copy_options=["FORMAT AS JSON 'auto'"]
    )

    load_songplays_table = PostgresOperator(
        task_id='Load_songplays_fact_table',
        postgres_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert
    )

    load_songs_table = PostgresOperator(
        task_id='Load_songs_table',
        postgres_conn_id="redshift",
        sql=SqlQueries.song_table_insert
    )

    load_users_table = PostgresOperator(
        task_id='Load_users_table',
        postgres_conn_id="redshift",
        sql=SqlQueries.user_table_insert
    )

    load_artists_table = PostgresOperator(
        task_id='Load_artists_table',
        postgres_conn_id="redshift",
        sql=SqlQueries.artist_table_insert
    )

    load_time_table = PostgresOperator(
        task_id='Load_time_table',
        postgres_conn_id="redshift",
        sql=SqlQueries.time_table_insert
    )

    @task()
    def check_data_quality():
        redshift_hook = RedshiftHook("redshift")
        tables = ["songplays", "songs", "artists", "time", "users"]
        for table in tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {num_records} records")

    run_quality_checks = PythonOperator(
        task_id='Run_data_quality_checks',
        python_callable=check_data_quality
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_trips_table
    create_trips_table >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_songs_table, load_artists_table, load_time_table, load_users_table]
    [load_songs_table, load_artists_table, load_time_table, load_users_table] >> run_quality_checks
    run_quality_checks >> end_operator

airflow_project = airflow_project()

