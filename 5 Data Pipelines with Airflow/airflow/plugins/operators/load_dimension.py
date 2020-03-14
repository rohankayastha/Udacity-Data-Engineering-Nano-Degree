from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'   
    delete_sql = "    DELETE FROM {};"
    insert_clause = "   INSERT INTO {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 copy_type="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id        
        self.table=table
        self.copy_type=copy_type
        self.insert_sql=insert_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.copy_type=='delete-load':
            self.log.info("Delete prior to loading data from S3 to Redshift table: {}".format(self.table))        
            self.log.info("Delete SQL:\n {}".format(LoadDimensionOperator.delete_sql.format(self.table)))        
            redshift.run(LoadDimensionOperator.delete_sql.format(self.table))
        
        self.log.info("Insert data from staging table to: {}".format(self.table))
        formatted_sql=LoadDimensionOperator.insert_clause.format(self.table)+self.insert_sql
        self.log.info("Insert SQL:\n {}".format(formatted_sql))        
        redshift.run(formatted_sql)
        
