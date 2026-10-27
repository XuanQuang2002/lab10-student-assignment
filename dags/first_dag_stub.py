import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='first_dag_stub',
    default_args=default_args_dict,
    catchup=False,
)

# task_one = DummyOperator(
#     task_id='get_bitcoin_price',
#     dag=first_dag,
#     depends_on_past=False,
# )

task_one = BashOperator(
    task_id='get_bitcoin_price',
    # The lab's CoinDesk hostname may be unavailable; use Coinbase as a fallback.
    bash_command="""
        if ! curl --fail --silent --show-error --location \
            --connect-timeout 5 --max-time 30 \
            https://api.coindesk.com/v1/bpi/currentprice.json \
            --output /tmp/bitcoin.json; then
            echo "CoinDesk unavailable; downloading BTC/USD data from Coinbase."
            curl --fail --silent --show-error --location \
                --connect-timeout 5 --max-time 30 \
                https://api.coinbase.com/v2/prices/BTC-USD/spot \
                --output /tmp/bitcoin.json
        fi
    """,
    dag=first_dag,
    depends_on_past=False,
)

# task_two = DummyOperator(
#     task_id='extract_usd_price',
#     dag=first_dag,
#     trigger_rule='all_success',
#     depends_on_past=False,
# )

task_two = BashOperator(
    task_id='extract_usd_price',
    bash_command="grep 'USD' /tmp/bitcoin.json > /tmp/bitcoin_usd.txt",
    dag=first_dag,
)

# task_three = DummyOperator(
#     task_id='log_price',
#     dag=first_dag,
#     trigger_rule='all_success',
#     depends_on_past=False,
# )

task_three = BashOperator(
    task_id='log_price',
    bash_command='cat /tmp/bitcoin_usd.txt',
    dag=first_dag,
)

# task_four = DummyOperator(
#     task_id='cleanup',
#     dag=first_dag,
#     trigger_rule='all_success',
#     depends_on_past=False,
# )

task_four = BashOperator(
    task_id='cleanup',
    bash_command='rm -f /tmp/bitcoin.json /tmp/bitcoin_usd.txt',
    dag=first_dag,
)

# TODO 1: (Tùy chọn) Chuyển các DummyOperator ở trên thành BashOperator để thực hiện công việc thực tế (tham khảo file giải nếu cần).
# Nhiệm vụ: Tải giá Bitcoin (API: https://api.coindesk.com/v1/bpi/currentprice.json), trích xuất chữ "USD", in ra log, và xóa file tạm.

# TODO 2: Khai báo DAG Dependencies (mối quan hệ phụ thuộc) giữa các task
# Ví dụ: task_one >> task_two
task_one >> task_two >> task_three >> task_four
