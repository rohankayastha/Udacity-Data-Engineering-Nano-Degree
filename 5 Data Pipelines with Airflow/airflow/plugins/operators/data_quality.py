from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id        
        self.table_list=table_list
        self.table_list=table_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Check here to verify that at least one record was found
        # Raise an error if less than one record is found
        
        for table in self.table_list:
            self.log.info("Data Check for table: {}".format(table))        
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")                           
            num_records = records[0][0]
            if num_records<1:
                raise ValueError(f"Data quality check failed. No records found in table {table}")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")        