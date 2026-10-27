import datetime
import urllib.request as request
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def _get_random_user(epoch, output_folder):
    request.urlretrieve(url="https://randomuser.me/api/",
        filename=f"{output_folder}/{epoch}_user.json")

def _check_gender(epoch, output_folder):
    with open(f'{output_folder}/{epoch}_user.json', 'r') as f:
        data = json.load(f)
    gender = data['results'][0]['gender']
    if gender == 'female': 
        return 'process_female'
    else: 
        return 'process_male'

def prepare_user_sql(table_name, epoch, output_folder):
    # The slide's download task saves the complete API response in this file.
    with open(f"{output_folder}/{epoch}_user.json", "r") as user_file:
        user = json.load(user_file)["results"][0]
    full_name = f"{user['name']['first']} {user['name']['last']}"

    # PythonOperator automatically saves this returned dictionary in XCom.
    # Keep values separate from SQL so names containing apostrophes work safely.
    return {
        "sql": (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "(name VARCHAR, email VARCHAR);\n"
            f"INSERT INTO {table_name} (name, email) "
            "VALUES (%(name)s, %(email)s);"
        ),
        "parameters": {"name": full_name, "email": user["email"]},
    }


def process_female(epoch, output_folder):
    return prepare_user_sql("female_users", epoch, output_folder)


def process_male(epoch, output_folder):
    return prepare_user_sql("male_users", epoch, output_folder)

default_args_dict = {
    'start_date': datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

second_dag = DAG(
    dag_id='second_dag_stub',
    default_args=default_args_dict,
    schedule_interval="0 0 * * *",
    max_active_tasks=1,
    catchup=False,
    render_template_as_native_obj=True,
)

task_one = PythonOperator(
    task_id='get_random_user',
    dag=second_dag,
    python_callable=_get_random_user,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
)

task_two = BranchPythonOperator(
    task_id='check_gender',
    dag=second_dag,
    python_callable=_check_gender,
    op_kwargs={'epoch': '{{ execution_date.int_timestamp }}', "output_folder":
        "/opt/airflow/dags"},
)

task_three_a = PythonOperator(
    task_id="process_female",
    python_callable=process_female,
    op_kwargs={
        "epoch": "{{ execution_date.int_timestamp }}",
        "output_folder": "/opt/airflow/dags",
    },
    dag=second_dag,
)

task_three_b = PythonOperator(
    task_id="process_male",
    python_callable=process_male,
    op_kwargs={
        "epoch": "{{ execution_date.int_timestamp }}",
        "output_folder": "/opt/airflow/dags",
    },
    dag=second_dag,
)

task_four_a = PostgresOperator(
    task_id="insert_female",
    postgres_conn_id="postgres_default",
    sql="{{ ti.xcom_pull(task_ids='process_female')['sql'] }}",
    parameters={
        "name": "{{ ti.xcom_pull(task_ids='process_female')['parameters']['name'] }}",
        "email": "{{ ti.xcom_pull(task_ids='process_female')['parameters']['email'] }}",
    },
    dag=second_dag,
)

task_four_b = PostgresOperator(
    task_id="insert_male",
    postgres_conn_id="postgres_default",
    sql="{{ ti.xcom_pull(task_ids='process_male')['sql'] }}",
    parameters={
        "name": "{{ ti.xcom_pull(task_ids='process_male')['parameters']['name'] }}",
        "email": "{{ ti.xcom_pull(task_ids='process_male')['parameters']['email'] }}",
    },
    dag=second_dag,
)

join_tasks = EmptyOperator(
    task_id='join_tasks',
    dag=second_dag,
    trigger_rule='none_failed'
)

end = EmptyOperator(
    task_id='end',
    dag=second_dag,
    trigger_rule='none_failed'
)

# Fetch a user, select one branch, insert the user, then join the branches.
task_one >> task_two
task_two >> [task_three_a, task_three_b]

task_three_a >> task_four_a >> join_tasks
task_three_b >> task_four_b >> join_tasks

join_tasks >> end
