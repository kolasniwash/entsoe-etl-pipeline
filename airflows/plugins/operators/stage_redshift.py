from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{{}}'
            SECRET_ACCESS_KEY '{{}}'
            CSV 'auto'
            REGION 'us-west-2'
            """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting...')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        sql_stmt = StageToRedshiftOperator.COPY_SQL.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift_hook.run(sql_stmt)

        self.log.info(f'{self.table} Loaded')





