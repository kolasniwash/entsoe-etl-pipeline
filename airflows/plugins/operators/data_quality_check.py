from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_data_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_data_checks = sql_data_checks

    def execute(self, context):
        self.log.info('Data Quality Checking ...')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.sql_data_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            error_count = 0
            failing_tests = list()

            records = redshift_hook.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info(f'Data quality check test failures {error_count}')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info('All data quality checks passed')