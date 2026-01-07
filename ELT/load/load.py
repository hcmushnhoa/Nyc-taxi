import duckdb
from datetime import timedelta
from dotenv import load_dotenv
from airflow.decorators import task
load_dotenv()
MINIO_ENDPOINT = "minio:9000"  # Lưu ý: Không có http://
MINIO_ACCESS_KEY = "minio"         # User đăng nhập MinIO
MINIO_SECRET_KEY = "minio123"
@task(
    task_id="merge_to_silver_duckdb")
def merge_to_silver_duckdb_native(s3_path_bronze, taxi_type):
    #con = duckdb.connect('/opt/airflow/data/nyc_taxi.duckdb')
    #DB_PATH = "/tmp/nyc_taxi.duckdb"
    with duckdb.connect('/opt/airflow/duckdb_data/nyc_taxi.duckdb') as con:
        try:
            # Cấu hình Extension để đọc S3/MinIO
            con.sql("INSTALL httpfs; LOAD httpfs;")
            # Thiết lập biến môi trường cho DuckDB session
            con.sql(f"""
                SET s3_region='us-east-1';
                SET s3_endpoint='{MINIO_ENDPOINT}';
                SET s3_access_key_id='{MINIO_ACCESS_KEY}';
                SET s3_secret_access_key='{MINIO_SECRET_KEY}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)

            table_name = f"trips_{taxi_type}"

            # --- CHIẾN THUẬT SCHEMA EVOLUTION (Giữ nguyên) ---
            # Kiểm tra bảng tồn tại chưa
            table_exists = con.sql(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

            if not table_exists:
                print(f"Bảng {table_name} chưa có. Tạo mới từ schema file Parquet...")
                con.sql(f"""
                    CREATE TABLE {table_name} AS
                    SELECT
                        md5(concat('{s3_path_bronze}', cast(row_number() over () as VARCHAR))) as unique_row_id,
                        '{s3_path_bronze}' as source_file,
                        * FROM read_parquet('{s3_path_bronze}')
                    WHERE 1=0; -- Chỉ tạo cấu trúc, không load data
                        """)
            if taxi_type == 'yellow':
                pickup_col = 'tpep_pickup_datetime'
                dropoff_col = 'tpep_dropoff_datetime'
            elif taxi_type == 'green':
                pickup_col = 'lpep_pickup_datetime'
                dropoff_col = 'lpep_dropoff_datetime'
            else:
                # Fallback cho các loại khác nếu có (fhv, etc.)
                pickup_col = 'pickup_datetime'
                dropoff_col = 'dropoff_datetime'
            # --- 4. Logic Merge (Giữ nguyên) ---
            print("Đang tạo Staging View...")
            con.sql(f"""
                    CREATE OR REPLACE TEMP VIEW stg_current_load AS
                    SELECT
                        -- Tạo Hash ID: Chỉ dùng đúng cột của loại xe đó
                        md5(concat(
                            coalesce(cast({pickup_col} as VARCHAR), ''),
                            coalesce(cast({dropoff_col} as VARCHAR), ''),
                            coalesce(cast(PULocationID as VARCHAR), ''),
                            coalesce(cast(trip_distance as VARCHAR), '')
                        )) as unique_row_id,
                        '{s3_path_bronze}' as source_file,
                        * FROM read_parquet('{s3_path_bronze}', union_by_name=True)
                """)

            # Xóa cũ (Deduplicate) sau đó mới merge từ staging table vào
            print("Đang xóa dữ liệu cũ trùng lặp...")
            con.sql(f"""
                DELETE FROM {table_name}
                WHERE unique_row_id IN (SELECT unique_row_id FROM stg_current_load)
            """)

            # Insert mới
            print("Đang Insert dữ liệu mới...")
            con.sql(f"""
                INSERT INTO {table_name}
                SELECT * FROM stg_current_load
            """)

            total_rows = con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            print(f"--- [Success] Done. Total rows: {total_rows} ---")

        except Exception as e:
            print(f"Lỗi DuckDB: {e}")
            raise e
