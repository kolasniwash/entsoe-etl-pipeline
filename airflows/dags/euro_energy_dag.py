from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageCSVToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import EuroEnergyQueries

default_args = {
    'owner': 'nicholas',
    'start_date': datetime(2020, 5, 27, 0,0,0),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False
}

dag = DAG('euro-energy-etl',
          default_args=default_args,
          description='Builds a redshift data warehouse from s3.',
          schedule_interval='0 5 * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


stage_demand_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_energy_loads',
    dag=dag,
    table='staging_energy_loads',
    create_table_sql=EuroEnergyQueries.stage_energy_loads,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket='s3://energy-etl-processed/total_demand'
)

stage_installed_capacity_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_installed_capacity',
    dag=dag,
    table="staging_installed_cap",
    create_table_sql=EuroEnergyQueries.stage_installed_capacity,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://energy-etl-processed/installed_capacity"
)

stage_generation_to_redshift = StageCSVToRedshiftOperator(
    task_id='stage_generation',
    dag=dag,
    table="staging_energy_generation",
    create_table_sql=EuroEnergyQueries.stage_energy_generation,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://energy-etl-processed/total_generation"
)

stage_day_ahead_prices_to_redshift = StageCSVToRedshiftOperator(
    task_id='staging_day_ahead_prices',
    dag=dag,
    table="staging_day_ahead_prices",
    create_table_sql=EuroEnergyQueries.stage_day_ahead_prices,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://energy-etl-processed/day_ahead_prices"
)

stage_quality_checks = DataQualityOperator(
    task_id='stage_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_data_checks=[
        {"check_sql": "SELECT count(*) FROM staging_day_ahead_prices WHERE event_date is NULL",
         "expected_result": 0},
        {"check_sql": "SELECT count(*) FROM staging_energy_generation WHERE event_date is NULL",
         "expected_result": 0},
        {"check_sql": "SELECT count(*) FROM staging_installed_cap WHERE area_date is NULL",
         "expected_result": 0},
        {"check_sql": "SELECT count(*) FROM staging_energy_loads WHERE event_date is NULL",
         "expected_result": 0}
    ]
)

## countries csv is already unique. No need to load after stage.
create_load_countries_table = StageCSVToRedshiftOperator(
    task_id='load_countries_table',
    dag=dag,
    table='countries',
    create_table_sql=EuroEnergyQueries.create_countries,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='s3://energy-etl-processed/countries'
)

load_energy_loads_table = LoadFactOperator(
    task_id='load_energy_loads_table',
    dag=dag,
    table_id='energy_loads',
    create_table_sql=EuroEnergyQueries.create_energy_loads,
    redshift_conn_id='redshift',
    sql_select=EuroEnergyQueries.energy_loads_table_insert
)

fact_table_size_check = DataQualityOperator(
    task_id='fact_table_size_check',
    dag=dag,
    redshift_conn_id='redshift',
    sql_data_checks=[
        {"check_sql": """SELECT CASE WHEN count(*)>1000000 THEN true ELSE false END FROM energy_loads""",
         "expected_result": 'true'}
    ]
)

load_installed_capacity_table = LoadDimensionOperator(
    task_id='load_installed_generation',
    dag=dag,
    table_id='installed_capacity',
    redshift_conn_id='redshift',
    sql_select=EuroEnergyQueries.installed_capacity_insert
)

load_times_table = LoadDimensionOperator(
    task_id='load_times_table',
    dag=dag,
    table_id='times',
    redshift_conn_id='redshift',
    sql_select=EuroEnergyQueries.times_table_insert
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> [stage_demand_to_redshift,
                   stage_installed_capacity_to_redshift,
                   stage_generation_to_redshift,
                   stage_day_ahead_prices_to_redshift] >> stage_quality_checks

stage_quality_checks >> load_energy_loads_table >> fact_table_size_check

fact_table_size_check >> [load_installed_capacity_table,
                            load_times_table] >> end_operator
