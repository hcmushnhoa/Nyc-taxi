from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import timedelta
import pendulum
import os
import duckdb
import shutil
from ELT.extract.extract import ingest_to_minio_bronze
from ELT.load.load import merge_to_silver_duckdb_native

DATASET_YELLOW = Dataset("duckdb://nyc_taxi_yellow")
DATASET_GREEN = Dataset("duckdb://nyc_taxi_green")

GET_YEAR = "{% if params.use_manual_date %}{{ params.manual_year }}{% else %}{{ logical_date.strftime('%Y') }}{% endif %}"
GET_MONTH = "{% if params.use_manual_date %}{{ params.manual_month }}{% else %}{{ logical_date.strftime('%m') }}{% endif %}"
# Ingest task
def create_ingest_dag(taxi_type):
    dag_id = f'nyc_taxi_{taxi_type}_ingest'
    target_dataset = DATASET_YELLOW if taxi_type == 'yellow' else DATASET_GREEN

    with DAG(
            dag_id=dag_id,
            schedule='@monthly',
            start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
            end_date=pendulum.datetime(2023, 10, 31, tz="UTC"),
            catchup=True,
            max_active_runs=1,
            tags=['ingest', 'duckdb', taxi_type],
            default_args={'retries': 2, 'retry_delay': timedelta(minutes=1), 'depends_on_past': True},
            params={
                "use_manual_date": Param(False, type="boolean", title="Chạy thủ công?"),
                "manual_year": Param("2024", type="string", title="Năm"),
                "manual_month": Param("01", type="string", title="Tháng", enum=[f"{i:02}" for i in range(1, 13)]),
            }
    ) as dag:
        s3_path = ingest_to_minio_bronze(
            taxi_type=taxi_type,
            year=GET_YEAR,
            month=GET_MONTH
        )

        # Load Raw & Bắn tín hiệu Dataset
        merge_to_silver_duckdb_native.override(
            task_id=f'load_{taxi_type}_raw',
            pool='duckdb_write_pool',
            outlets=[target_dataset]
        )(
            s3_path_bronze=s3_path,
            taxi_type=taxi_type
        )
    return dag


yellow_ingest_dag = create_ingest_dag('yellow')
green_ingest_dag = create_ingest_dag('green')

# Transform task and publish view

# Định nghĩa hàm Publish View (Python Task)
@task(task_id="publish_view_db")
def publish_to_view_layer():
    # Đường dẫn nội bộ (DB gốc - nơi dbt chạy)
    INTERNAL_DB = '/opt/airflow/duckdb_data/nyc_taxi.duckdb'

    # Path xuất ra (map với host ./dbeaver_view để xem connect data vào dbeaver)
    EXPORT_PATH = '/opt/airflow/export_view/nyc_taxi_view.duckdb'

    print(f"Export from {INTERNAL_DB} to {EXPORT_PATH}...")

    try:
        # Lưu ý: check point cần write, nên read_only = False
        with duckdb.connect(INTERNAL_DB, read_only=False) as con:
            con.sql("CHECKPOINT")
            print("Success checkpoint")
    except Exception as e:
        print(f"Warn Checkpoint: {e}")

    # xóa file cũ
    if os.path.exists(EXPORT_PATH):
        try:
            os.remove(EXPORT_PATH)
            print("Đã xóa file view cũ.")
        except OSError:
            print("Không thể xóa file cũ (Streamlit đang giữ?). Sẽ thử ghi đè.")


    try:
        # shutil.copy2 giúp copy file giữ nguyên metadata
        shutil.copy2(INTERNAL_DB, EXPORT_PATH)
        print(f"copy thành công sang: {EXPORT_PATH}")
    except Exception as e:
        print(f"Lỗi khi copy file: {e}")
        raise e

with DAG(
        dag_id='nyc_taxi_transform',
        # Chạy khi MỘT TRONG HAI dataset được cập nhật
        schedule=[DATASET_YELLOW, DATASET_GREEN],
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=['dbt', 'transform', 'gold', 'docs']
) as dbt_dag:
    # dbt build
    dbt_build = BashOperator(
        task_id='dbt_build_all',
        bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt build -t prod --profiles-dir .',
        pool='duckdb_write_pool'  # Dùng chung pool để tránh xung đột với Ingest
    )

    # 2. dbt Docs
    # tạo file index.html trong /opt/airflow/dbt_project/target/
    dbt_docs = BashOperator(
        task_id='dbt_generate_docs',
        bash_command='cd /opt/airflow/dbt_project && dbt docs generate -t prod --profiles-dir .',
        pool='duckdb_write_pool'
    )

    # 3. Publish View (Xuất file ra ngoài cho DBeaver/Streamlit)
    publish_task = publish_to_view_layer()

    dbt_build >> dbt_docs >> publish_task