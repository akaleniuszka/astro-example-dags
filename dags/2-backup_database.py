from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def exportar_base_datos(**kwargs):
    # Get connection to the database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    # Get the connection to the database
    conn = pg_hook.get_conn()
    # Get the cursor
    cursor = conn.cursor()

    # Get the table name
    copy_to_csv_query = """
        COPY 
            (
                SELECT
                    customer.first_name,
                    customer.last_name,
                    film.title,
                    category.name AS category_name,
                    payment.amount,
                    payment.payment_date
                FROM public.rental
                INNER JOIN public.payment
                    ON rental.rental_id = payment.rental_id
                INNER JOIN public.customer
                    ON rental.customer_id = customer.customer_id
                INNER JOIN public.inventory
                ON rental.inventory_id = inventory.inventory_id
                INNER JOIN public.film
                ON inventory.film_id = film.film_id
                INNER JOIN public.film_category
                ON film.film_id = film_category.film_id
                INNER JOIN public.category
                ON film_category.category_id = category.category_id
            ) 
        TO 
            STDOUT 
        WITH 
            CSV
            DELIMITER ',' 
            HEADER
    """

    print("Se va a ejecutar el siguiente query: ", copy_to_csv_query)

    # Execute the query
    with open('rental.csv', 'w') as f:
        cursor.copy_expert(
            copy_to_csv_query, 
            f
        )
        cursor.close()

    print("Se ha exportado la base de datos correctamente")

    print("Se va a subir el archivo CSV a un contenedor de almacenamiento de Azure")

    azurehook = WasbHook(wasb_conn_id='azure_storage')

    timestamp = datetime.now().strftime('%Y-%m-%d')

    azurehook.load_string(
        string_data=open('rental.csv').read(),
        container_name='backup',
        blob_name='rental_' + timestamp + '.csv'
    )

    print("Se ha subido el archivo CSV a un contenedor de almacenamiento de Azure")
    
    return

dag1 = DAG(
    dag_id='2-backup_database',
    default_args=default_args,
    description='Copia de la base de datos a un archivo CSV',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    tags=['backup', 'database', 'csv', 'postgres'],
)

start = DummyOperator(
    task_id='start',
    dag=dag1
)

end = DummyOperator(
    task_id='end',
    dag=dag1
)

exportar_base_datos_task = PythonOperator(
    task_id='exportar_base_datos',
    python_callable=exportar_base_datos,
    op_kwargs={'postgres_conn_id': 'Aiven_DB_Conn'},
    dag=dag1
)

start >> exportar_base_datos_task >> end
