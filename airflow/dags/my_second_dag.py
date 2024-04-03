from airflow.operators.python import PythonOperator
from airflow import DAG

def my_write(data: str, file_name: str):
    with open(file_name, 'w') as f:
        f.write(data)
        f.write('\n')

def my_print(file_name: str):
    with open(file_name, 'r') as f:
        data = f.read()
        print(data)
        return data

with DAG(
    dag_id='my_second_dag',
    schedule=None
) as my_dag:
    first_task = PythonOperator(
        task_id = 'first_task',
        python_callable=my_write,
        dag=my_dag,
        op_kwargs={'data': 'Ya tebya kvaaa <3', 'file_name': 'file.txt'}
    )

    second_task = PythonOperator(
        task_id='second_task',
        python_callable=my_print,
        dag=my_dag,
        op_kwargs={'file_name': 'file.txt'}
    )

    first_task >> second_task
