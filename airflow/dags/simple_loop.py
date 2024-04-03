from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow import DAG

number = 4

def foo(i: int):
    print(i**2)
    return i**2

with DAG(
    dag_id='loop_dag',
    schedule=None
) as my_dag:
    first_task = EmptyOperator(
        task_id = 'eee',
        dag=my_dag
    )

    for i in range(number):
        child_task = PythonOperator(
            task_id=f'child_task_{i}',
            dag=my_dag,
            op_kwargs={'i': i},
            python_callable=foo
        )

        first_task >> child_task