from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

# Importa el subs-DAG desde otro archivo
from subsDAG import subsDAG

# Define una función Python que se ejecutará en PythonOperator
def my_python_function(**kwargs):
    # Realiza alguna operación
    result = 10 * 5
    # Empuja el resultado a XCom
    kwargs['ti'].xcom_push(key='my_result', value=result)

# Crea el DAG principal
with DAG(
    'miDAG',
    schedule_interval=None,  # Puedes configurar el intervalo de programación
    start_date=days_ago(1),  # Fecha de inicio
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Define un grupo de tareas para realizar operaciones
    with TaskGroup('operaciones') as operaciones:
        python_task = PythonOperator(
            task_id='my_python_task',
            python_callable=my_python_function,
            provide_context=True,
        )

    end = DummyOperator(task_id='end')

    # Define una tarea para realizar una división por Branch
    def branch_func(**kwargs):
        # Recupera el valor de XCom
        result = kwargs['ti'].xcom_pull(key='my_result', task_ids='operaciones.my_python_task')
        if result > 25:
            return 'branch_a'
        else:
            return 'branch_b'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
        provide_context=True,
    )

    # Define dos DummyOperators para las ramas
    branch_a = DummyOperator(task_id='branch_a')
    branch_b = DummyOperator(task_id='branch_b')

    trigger_subsDAG = TriggerDagRunOperator(
        task_id='trigger_subsDAG',
        trigger_dag_id="subsDAG",  # Nombre del subs-DAG
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,  # Regla de activación
    )

    # Define las dependencias entre las tareas
    start >> operaciones >> branch_task
    branch_task >> [branch_a, branch_b]
    branch_a >> end
    branch_b >> end
    trigger_subsDAG >> start

if __name__ == "__main__":
    dag.cli()