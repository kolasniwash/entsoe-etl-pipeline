from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    sql = """BEGIN;
        INSERT INTO {} (
            {}
        );
        COMMIT;"""

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_id='',
                 create_table_sql=None,
                 redshift_conn_id='',
                 sql_select='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table_id
        self.create_table_sql=create_table_sql
        self.redshift_conn_id = redshift_conn_id
        self.sql_select = sql_select

    def execute(self, context):
        self.log.info(f'Loading table {self.table}')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.create_table_sql is not None:
            redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}")
            redshift_hook.run(f"{self.create_table_sql}")

        sql_stmt = LoadFactOperator.sql.format(
            self.table,
            self.sql_select
        )
        redshift_hook.run(sql_stmt)

        self.log.info(f'Table Loaded {self.table}')


