from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageCSVToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

# from helpers import EuroEnergyQueries


## MVP Dag outline:
## Copy data from S3 into staging tables
##  - SQL queries file
##  - Use stage to redshift operator
## Upsert data from Staging into Data Warehouse
##  - SQL queries file
##  - Use load dimension operator
##  - Use data quality operator
## Run data checks
##  - Data quality operator


## MVP V2 Dag outline
## Copy data from S3 working > clean with Spark & EMR cluster > Save in S3 processed zone
##  - Spark cleaning py file
##  - helper functions to get and upload to S3
## Copy data from S3 Processed zone into staging tables
##  - SQL queries file
##  - Use stage to redshift operator
## Upsert data from Staging into Data Warehouse
##  - SQL queries file
##  - Use load dimension operator
##  - Use data quality operator
## Run data checks
##  - Data quality operator


default_args = {
    'owner': 'nicholas',
    'start_date': datetime.now(),  # datetime(2020, 5, 21, 3, 0, 0, 0),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('euro-energy-etl',
          default_args=default_args,
          description='Builds a redshift data warehouse from s3.',
          schedule_interval='0 3 * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

data_process_clean = PythonOperator(
    task_id='data_process_clean',
    dag=dag
)


stage_demand_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_energy_demands',
    dag=dag,
    table='stage_energy_demands',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket='s3://euro-energy-data/total_demand'
)

stage_installed_capacity_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_intalled_capacity',
    dag=dag,
    table="stage_intalled_capacity",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://euro-energy-data/installed_capacity"
)

stage_generation_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_generation',
    dag=dag,
    table="stage_generation",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://euro-energy-data/generation"
)

## countries csv is already unique. No need to stage. Load directly.
create_load_countries_table = StageCSVToRedshiftOperator(
    task_id='load_countries_table',
    dag=dag,
    table='load_countries_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='s3://euro-energy-data/countries'
)

load_energy_loads_table = LoadFactOperator(
    task_id='load_energy_loads_table',
    dag=dag,
    table_id='energy_loads',
    redshift_conn_id='redshift',
    sql_select=None  # EuroEnergyQueries.energy_loads_table_insert
)

load_generation_table = LoadDimensionOperator(
    task_id='load_generation_table',
    dag=dag,
    table_id='generation',
    redshift_conn_id='redshift',
    sql=None  # EuroEnergyQueries.generation_table_insert
)

load_times_table = LoadDimensionOperator(
    task_id='load_times_table',
    dag=dag,
    table_id='times',
    redshift_conn_id='redshift',
    sql=None  # EuroEnergyQueries.times_table_insert
)

load_countries_table = LoadDimensionOperator(
    task_id='load_countries_table',
    dag=dag,
    table_id='countries',
    redshift_conn_id='redshift',
    sql=None  # EuroEnergyQueries.countries_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    sql_data_checks=[
        {"check_sql": "SELECT count(user_id) FROM users WHERE user_id is NULL",
         "expected_result": 0},
        {"check_sql": "SELECT count(artist_id) FROM artists WHERE artist_id is NULL",
         "expected_result": 0},
        {"check_sql": "SELECT count(song_id) FROM songs WHERE song_id is NULL",
         "expected_result": 0},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_demand_to_redshift,
                   stage_installed_capacity_to_redshift,
                   stage_generation_to_redshift] >> load_energy_loads_table

load_energy_loads_table >> [load_generation_table,
                            load_times_table,
                            load_countries_table] >> run_quality_checks

run_quality_checks >> end_operator

