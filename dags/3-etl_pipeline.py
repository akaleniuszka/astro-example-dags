# Import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago

# DAG arguments

default_args = {
    'owner': 'Alfredo Kaleniuszka',
    'start_date': days_ago(0),
    'email': ['alfredo.kaleniuszka@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define function to move the data to Azure container
def move_to_azure_container(**kwargs):
    print("Moviendo el archivo "+kwargs['file']+" a Azure container")
    # Create a WasbHook object
    wasb_hook = WasbHook(wasb_conn_id='azure_storage')
    timestamp = timedelta.now().strftime('%Y-%m-%d')
    if 'extracted' in kwargs['file']:
        blobfilename = f"extracted_data_{timestamp}.csv"
    else:
        blobfilename = f"transformed_data_{timestamp}.csv"
    
    # Upload the file to the container
    wasb_hook.load_file(
        filename=kwargs['file'],
        container_name='backup',
        blob_name=blobfilename
    )

    print("Se ha subido el archivo "+kwargs['file']+" a Azure container")

    return

# DAG definition

dag = DAG(
    '3-ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = default_args,
    description = 'Apache Airflow ETL pipeline to extract, transform and load toll data',
)

# Task definitions
## Create a task to unzip data

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -zxvf /usr/local/airflow/dags/final/tolldata.tgz -C /usr/local/airflow/dags/final',
    dag = dag,
)

## Create a task to extract data from csv file

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 /usr/local/airflow/dags/final/vehicle-data.csv > /usr/local/airflow/dags/final/staging/csv_data.csv',
    dag = dag,
)

## Create a task to extract data from tsv file

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -d" " -f5-7 /usr/local/airflow/dags/final/tollplaza-data.tsv | tr -d "\r" | tr "[:blank:]" "," > /usr/local/airflow/dags/final/staging/tsv_data.csv',
    dag = dag,
)

## Create a task to extract data from fixed width file

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -b59-67 </usr/local/airflow/dags/final/payment-data.txt | tr " " ","> /usr/local/airflow/dags/final/staging/fixed_width_data.csv',
    dag = dag,
)

##  Create a task to consolidate data extracted from previous tasks

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste /usr/local/airflow/dags/final/staging/csv_data.csv /usr/local/airflow/dags/final/staging/tsv_data.csv /usr/local/airflow/dags/final/staging/fixed_width_data.csv > /usr/local/airflow/dags/final/staging/extracted_data.csv',
    dag = dag,
)

## Transform and load the data

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'cut -d "," -f4 /usr/local/airflow/dags/final/staging/extracted_data.csv | tr "[a-z]" "[A-Z]" > /usr/local/airflow/dags/final/staging/transformed_data.csv',
    dag = dag,
)

move_extracted_to_azure_container_task = PythonOperator(
    task_id = 'move_to_azure_container',
    python_callable = move_to_azure_container,
    op_kwargs={'file': '/usr/local/airflow/dags/final/staging/extracted_data.csv'},
    dag = dag,
)

move_transformed_to_azure_container = PythonOperator(
    task_id = 'move_to_azure_container',
    python_callable = move_to_azure_container,
    op_kwargs={'file': '/usr/local/airflow/dags/final/staging/transformed_data.csv'},
    dag = dag,
)

## Define the task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> move_extracted_to_azure_container_task >> transform_data >> move_transformed_to_azure_container