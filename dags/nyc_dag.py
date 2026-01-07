# from airflow import DAG
# from airflow.models.param import Param
# from airflow.decorators import task
# from datetime import timedelta
# import pendulum
#
# # Import tasks từ module của bạn
# from ELT.extract.extract import ingest_to_minio_bronze
# from ELT.load.load import merge_to_silver_duckdb_native
#
# # --- CẤU HÌNH LOGIC CHỌN NGÀY ---
# # Vẫn giữ logic Manual Override của bạn
# GET_YEAR = "{% if params.use_manual_date %}{{ params.manual_year }}{% else %}{{ logical_date.strftime('%Y') }}{% endif %}"
# GET_MONTH = "{% if params.use_manual_date %}{{ params.manual_month }}{% else %}{{ logical_date.strftime('%m') }}{% endif %}"
#
#
# # --- HÀM TẠO DAG (DAG FACTORY) ---
# def create_taxi_dag(taxi_type):
#     """
#     Hàm này sẽ sinh ra một DAG object riêng biệt cho từng loại taxi
#     """
#     dag_id = f'nyc_taxi_{taxi_type}_etl'
#
#     with DAG(
#             dag_id=dag_id,
#             schedule='@monthly',
#             start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#             end_date=pendulum.datetime(2023, 10, 31, tz="UTC"),
#             catchup=True,
#             max_active_runs=1,  # Giữ thứ tự tuần tự cho từng loại xe
#             tags=['duckdb', 'minio', 'hybrid', taxi_type],
#             default_args={
#                 'retries': 2,
#                 'retry_delay': timedelta(minutes=1),
#                 'depends_on_past': True  # Tháng sau đợi tháng trước (của cùng loại xe)
#             },
#             # Params riêng cho từng DAG
#             params={
#                 "use_manual_date": Param(
#                     default=False,
#                     type="boolean",
#                     title="⚡ Chế độ chạy thủ công?",
#                     description="Tích để nhập năm/tháng bằng tay."
#                 ),
#                 "manual_year": Param(default="2024", type="string", title="Năm (Manual)"),
#                 "manual_month": Param(default="01", type="string", title="Tháng (Manual)",
#                                       enum=[f"{i:02}" for i in range(1, 13)]),
#                 # Không cần param chọn taxi_type nữa vì DAG này đã cố định loại xe
#             }
#     ) as dag:
#         # 1. Task Ingest (Chạy song song thoải mái)
#         # Truyền taxi_type cứng vào đây
#         s3_path = ingest_to_minio_bronze(
#             taxi_type=taxi_type,
#             year=GET_YEAR,
#             month=GET_MONTH
#         )
#         # 2. Task Merge (CẦN POOL ĐỂ TRÁNH LOCK)
#         # Sử dụng .override() để gán Pool
#         merge_to_silver_duckdb_native.override(
#             task_id=f'merge_{taxi_type}_to_duckdb',
#             pool='duckdb_write_pool'  # <--- CHÌA KHÓA QUAN TRỌNG
#         )(
#             s3_path_bronze=s3_path,
#             taxi_type=taxi_type
#         )
#     return dag
#
#
# # --- KHỞI TẠO CÁC DAG ---
# # Airflow sẽ tìm thấy 2 biến toàn cục này và đăng ký thành 2 DAGs trên UI
# yellow_taxi_dag = create_taxi_dag('yellow')
# green_taxi_dag = create_taxi_dag('green')
# from airflow import DAG
# from airflow.models.param import Param
# from airflow.operators.bash import BashOperator
# from airflow.datasets import Dataset  # <--- Import mới quan trọng
# from datetime import timedelta
# import pendulum
#
# # Import tasks từ module của bạn
# from ELT.extract.extract import ingest_to_minio_bronze
# from ELT.load.load import merge_to_silver_duckdb_native
#
# # --- 1. ĐỊNH NGHĨA DATASETS (TÍN HIỆU) ---
# # Đây là "địa chỉ" để Airflow biết ai xong việc
# DATASET_YELLOW = Dataset("duckdb://nyc_taxi_yellow")
# DATASET_GREEN = Dataset("duckdb://nyc_taxi_green")
#
# # --- CẤU HÌNH LOGIC NGÀY ---
# GET_YEAR = "{% if params.use_manual_date %}{{ params.manual_year }}{% else %}{{ logical_date.strftime('%Y') }}{% endif %}"
# GET_MONTH = "{% if params.use_manual_date %}{{ params.manual_month }}{% else %}{{ logical_date.strftime('%m') }}{% endif %}"
#
#
# # --- 2. HÀM TẠO DAG INGEST (PRODUCER) ---
# def create_ingest_dag(taxi_type):
#     dag_id = f'nyc_taxi_{taxi_type}_ingest'
#
#     # Chọn đúng Dataset dựa trên loại xe
#     target_dataset = DATASET_YELLOW if taxi_type == 'yellow' else DATASET_GREEN
#
#     with DAG(
#             dag_id=dag_id,
#             schedule='@monthly',
#             start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#             end_date=pendulum.datetime(2023, 10, 31, tz="UTC"),
#             catchup=True,
#             max_active_runs=1,
#             tags=['ingest', 'duckdb', taxi_type],
#             default_args={
#                 'retries': 2,
#                 'retry_delay': timedelta(minutes=1),
#                 'depends_on_past': True
#             },
#             params={
#                 "use_manual_date": Param(False, type="boolean", title="Chạy thủ công?"),
#                 "manual_year": Param("2024", type="string", title="Năm"),
#                 "manual_month": Param("01", type="string", title="Tháng", enum=[f"{i:02}" for i in range(1, 13)]),
#             }
#     ) as dag:
#         # Bước 1: Tải file về MinIO
#         s3_path = ingest_to_minio_bronze(
#             taxi_type=taxi_type,
#             year=GET_YEAR,
#             month=GET_MONTH
#         )
#
#         # Bước 2: Load vào DuckDB Raw + BẮN TÍN HIỆU (Outlets)
#         # Lưu ý: Lúc này hàm merge của bạn nên chỉ đơn thuần là Load Raw (như đã bàn)
#         # Nếu chưa sửa code load.py, cứ để nó chạy tạm, dbt sẽ chạy đè lên sau.
#         merge_to_silver_duckdb_native.override(
#             task_id=f'load_{taxi_type}_raw',
#             pool='duckdb_write_pool',  # Vẫn cần pool để tránh lỗi lock khi ghi
#             outlets=[target_dataset]  # <--- Báo hiệu: "Tôi đã nạp xong data này!"
#         )(
#             s3_path_bronze=s3_path,
#             taxi_type=taxi_type
#         )
#
#     return dag
#
#
# # --- 3. KHỞI TẠO DAG INGEST ---
# yellow_ingest_dag = create_ingest_dag('yellow')
# green_ingest_dag = create_ingest_dag('green')
#
# # --- 4. TẠO DAG DBT TRANSFORM (CONSUMER) ---
# # DAG này sẽ tự chạy khi CÓ TÍN HIỆU từ dataset
# with DAG(
#         dag_id='nyc_taxi_dbt_transform',
#         # Schedule này nghĩa là: Chờ 1 trong 2, hoặc cả 2 (tùy config dataset trigger)
#         # Mặc định Airflow: Khi Dataset được update, DAG này sẽ trigger.
#         schedule=[DATASET_YELLOW, DATASET_GREEN],
#         start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#         catchup=False,  # dbt không cần chạy bù lịch sử, nó luôn build trên data hiện tại
#         max_active_runs=1,  # Chỉ cho phép 1 tiến trình dbt chạy tại 1 thời điểm
#         tags=['dbt', 'transform', 'gold']
# ) as dbt_dag:
#     # Task chạy dbt build
#     # Lưu ý: -t prod để dùng profile production kết nối đúng host
#     dbt_build = BashOperator(
#         task_id='dbt_build_all',
#         bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt build -t prod --profiles-dir .'
#     )
#     # (Tùy chọn) Task xuất file View ra cho DBeaver xem (như đã bàn trước đó)
#     # publish_view_task = ... (Code Python task publish)
#     # dbt_build >> publish_view_task
#     # thêm dbt docs generate
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

# Import tasks từ module của bạn
from ELT.extract.extract import ingest_to_minio_bronze
from ELT.load.load import merge_to_silver_duckdb_native

# --- 1. ĐỊNH NGHĨA DATASETS ---
DATASET_YELLOW = Dataset("duckdb://nyc_taxi_yellow")
DATASET_GREEN = Dataset("duckdb://nyc_taxi_green")

# --- CẤU HÌNH ---
GET_YEAR = "{% if params.use_manual_date %}{{ params.manual_year }}{% else %}{{ logical_date.strftime('%Y') }}{% endif %}"
GET_MONTH = "{% if params.use_manual_date %}{{ params.manual_month }}{% else %}{{ logical_date.strftime('%m') }}{% endif %}"


# ==========================================
# DAG 1 & 2: INGESTION (PRODUCER)
# ==========================================
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


# ==========================================
# DAG 3: TRANSFORM & PUBLISH (CONSUMER)
# ==========================================

# Định nghĩa hàm Publish View (Python Task)
@task(task_id="publish_view_db")
def publish_to_view_layer():
    # Đường dẫn nội bộ (DB gốc - nơi dbt chạy)
    INTERNAL_DB = '/opt/airflow/duckdb_data/nyc_taxi.duckdb'

    # Đường dẫn xuất ra (Map với host ./dbeaver_view thông qua docker volume)
    EXPORT_PATH = '/opt/airflow/export_view/nyc_taxi_view.duckdb'

    print(f"🔄 Bắt đầu Export từ {INTERNAL_DB} sang {EXPORT_PATH}...")

    try:
        # Lưu ý: check point cần quyền ghi, nên read_only phải là False
        with duckdb.connect(INTERNAL_DB, read_only=False) as con:
            con.sql("CHECKPOINT")
            print("✅ Đã Checkpoint (Merge WAL) thành công.")
    except Exception as e:
        print(f"⚠️ Cảnh báo Checkpoint (Có thể bỏ qua nếu DB đang bận): {e}")

        # BƯỚC 2: XÓA FILE CŨ
    if os.path.exists(EXPORT_PATH):
        try:
            os.remove(EXPORT_PATH)
            print("🗑️ Đã xóa file view cũ.")
        except OSError:
            print("⚠️ Không thể xóa file cũ (Streamlit đang giữ?). Sẽ thử ghi đè.")

        # BƯỚC 3: COPY FILE (THAY THẾ VACUUM INTO)
    try:
        # shutil.copy2 giúp copy file giữ nguyên metadata
        shutil.copy2(INTERNAL_DB, EXPORT_PATH)
        print(f"✅ Đã copy file thành công sang: {EXPORT_PATH}")
    except Exception as e:
        print(f"❌ Lỗi khi copy file: {e}")
        raise e


with DAG(
        dag_id='nyc_taxi_dbt_transform',
        # Chạy khi MỘT TRONG HAI dataset được cập nhật
        schedule=[DATASET_YELLOW, DATASET_GREEN],
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=['dbt', 'transform', 'gold', 'docs']
) as dbt_dag:
    # 1. dbt Build (Chạy Model + Test)
    # Lưu ý đường dẫn: cd /opt/airflow/dbt_project (Khớp với docker-compose mới của bạn)
    dbt_build = BashOperator(
        task_id='dbt_build_all',
        bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt build -t prod --profiles-dir .',
        pool='duckdb_write_pool'  # Dùng chung pool để tránh xung đột với Ingest
    )

    # 2. dbt Docs (Tạo tài liệu)
    # Lệnh này sẽ tạo ra file index.html trong /opt/airflow/dbt_project/target/
    dbt_docs = BashOperator(
        task_id='dbt_generate_docs',
        bash_command='cd /opt/airflow/dbt_project && dbt docs generate -t prod --profiles-dir .',
        pool='duckdb_write_pool'
    )

    # 3. Publish View (Xuất file ra ngoài cho DBeaver/Streamlit)
    publish_task = publish_to_view_layer()

    # --- Luồng chạy ---
    dbt_build >> dbt_docs >> publish_task