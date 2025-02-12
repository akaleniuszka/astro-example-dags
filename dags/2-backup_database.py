from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import json

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Fetch and parse the DB_CREDENTIALS variable
db_credentials = json.loads(Variable.get("DB_CREDENTIALS"))
db_user = db_credentials["user"]
db_password = db_credentials["password"]
db_host = db_credentials["host"]
db_name = db_credentials["dbname"]
s3_bucket = Variable.get("s3_bucket")
backup_path = "./backup/backup.sql"

# Define the DAG
with DAG(
    'database_backup',
    default_args=default_args,
    description='Backup automatico de base de datos',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to back up the database
    backup_db = BashOperator(
        task_id='backup_db',
        bash_command=f'PGPASSWORD={db_password} pg_dump -U {db_user} -h {db_host} {db_name} > {backup_path}'
    )

    # Task to upload the backup to S3
    upload_to_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command=f'aws s3 cp {backup_path} s3://{s3_bucket}/backup.sql'
    )

    # Define task dependencies
    backup_db >> upload_to_s3