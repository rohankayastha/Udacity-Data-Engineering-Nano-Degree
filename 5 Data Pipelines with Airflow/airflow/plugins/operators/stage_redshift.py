import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("exec_date",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}  
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 exec_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_type=file_type
        self.exec_date=exec_date

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table: {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        
        rendered_date = self.exec_date.format(**context)
        date = datetime.datetime.strptime(rendered_date[0:19],"%Y-%m-%dT%H:%M:%S")
        year = date.year
        month = date.month
        
        self.log.info("Copying data from S3 to Redshift table: {}".format(self.table))
        # Additional options for file format
        file_type_options=''
        if self.file_type=='json':            
            file_type_options="FORMAT AS JSON 'auto';"
        if self.file_type=='csv':
            file_type_options="IGNOREHEADER 1 DELIMITER ','"                 
        
        # Specifying the path of the input files
        if self.table=='staging_events':
            s3_path = "s3://{}/{}/{}/{}".format(self.s3_bucket, self.s3_key, year, month)
        if self.table=='staging_songs':
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_type_options
        )
        redshift.run(formatted_sql)






