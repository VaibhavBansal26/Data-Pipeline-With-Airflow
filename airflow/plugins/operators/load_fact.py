from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",        
                 append_only = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Truncate {} fact table".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))  
            
        self.log.info("Inserting data from staging tables into {} fact table".format(self.table))
        
        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        redshift.run(insert_statement)
        self.log.info(f"Successfully loaded data into {self.table}")

