from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.tables = tables
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info('DataQualityOperator not implemented yet')