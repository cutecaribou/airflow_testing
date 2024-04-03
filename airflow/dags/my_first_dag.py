from airflow.operators.python import PythonOperator
from airflow import DAG


def my_fun(data: str):
    print('Nya~!', data)


def my_fail(data: str):
    raise Exception(data)


with DAG(
    dag_id="my_first_dag",
    schedule=None
) as mydag:
    my_first_task = PythonOperator(
        task_id="my_first_task",
        python_callable=my_fun,
        dag=mydag,
        op_kwargs={'data': 'penguin'}
    )

    my_second_task = PythonOperator(
        task_id="my_second_task",
        python_callable=my_fun,
        dag=mydag,
        op_kwargs={'data': 'Olenya'}
    )

    my_failed_task = PythonOperator(
        task_id="my_failed_task",
        python_callable=my_fail,
        dag=mydag,
        op_kwargs={'data': 'Bunk!'}
    )

    my_first_task  >> my_second_task
    my_failed_task >> my_second_task
