from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow import DAG
import os

# task_conf = [
#     8,
#     6,
#     3,
#     1,
#     9,
#     2
# ]
# def foo(i: int):
#     print(i**2)
#     return i**2


def fpass():
    pass


def path_join(root, file_name):
    return str(os.path.join(root, file_name))\
        .rstrip('/')                     \
        .replace('(', '_lp_') \
        .replace(')', '_rp_') \
        .replace('_', '__')   \
        .replace(' ', '_')    \
        .replace('-', '--')   \
        .replace('/', '-')    \
        .replace('~', '_tl_') \
        .replace('+', '_ps_') \
        .replace(':', '_cn_') \
        .replace('=', '_eq_')


dir_path = './'
with DAG(
    dag_id='loop_dag_4',
    schedule=None
) as my_dag:
    excluded = ['./venv', 'venv', './.git', '.git', '.idea', './.idea', 'logs']
    dir_tasks = {}

    for root, dirs, files in os.walk(dir_path):
        dirs[:] = [d for d in dirs if d not in excluded]

        root_path = path_join(root, '')

        if root_path in dir_tasks:
            root_task = dir_tasks[root_path]
        else:
            root_task = EmptyOperator(
                task_id=root_path,
                dag=my_dag,
                doc_md=root
            )
            dir_tasks[root_path] = root_task

        for child_dir in dirs:
            child_task = EmptyOperator(
                task_id=path_join(root, child_dir),
                doc_md=os.path.join(root, child_dir),
                dag=my_dag
            )
            dir_tasks[path_join(root, child_dir)] = child_task

            root_task >> child_task

        for file_name in files:
            tmp = path_join(root, file_name)
            task = PythonOperator(
                task_id=tmp,
                dag=my_dag,
                python_callable=fpass,
                doc_md=os.path.join(root, file_name),
            )
            root_task >> task
