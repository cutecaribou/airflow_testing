from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow import DAG

task_conf = [8, 6, 3, 1, 9, 2]
def foo(i: int):
    print(i**2)
    return i**2

with DAG(
    dag_id='blocks_of_tasks_1',
    schedule=None
) as my_dag:
    prev_last_task_in_block = None

    for idx, task_block in enumerate(task_conf):
        if prev_last_task_in_block:
            first_task_in_block = prev_last_task_in_block
        else:
            first_task_in_block = EmptyOperator(
                task_id=f'first_{idx}',
                dag=my_dag
            )
        last_task_in_block = EmptyOperator(
            task_id=f'last_{idx}',
            dag=my_dag
        )

        prev_last_task_in_block = last_task_in_block

        for i in range(task_block):
            child_task = PythonOperator(
                task_id=f'child_task_{idx}_{i}',
                dag=my_dag,
                op_kwargs={'i': i},
                python_callable=foo
            )

            first_task_in_block >> child_task >> last_task_in_block
        