from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))  
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} returned no results".format(table))
                raise ValueError("Data quality check failed. {}".format(table))
                
            record_count = records[0][0]
            if record_count == 0:
                self.log.error("No records present {}".format(table))
                raise ValueError("No records present  {}".format(table))
                
            self.log.info("Data quality on table {} check passed with {} records".format(table, record_count))