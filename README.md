# NYC Taxi Data Pipeline
## **Overview**
Dự án là một hệ thống data pipeline ELT (Extract - Load - Transform) 
toàn diện, được xây dựng để xử lý dữ liệu lịch sử chuyến đi của xe taxi tại 
New York (NYC TLC Trip Record Data). Hệ thống tập trung vào việc thu thập 
dữ liệu lưu trữ, chuẩn hóa và phân tích chuyên sâu để đưa ra các 
insight về hoạt động vận hành, hành vi khách hàng và doanh thu thông 
qua Dashboard trực quan
## **Project Structure**
```text
.
├── Dockerfile
├── ELT
│   ├── extract
│   │   ├── crawl_file.ipynb
│   │   └── extract.py
│   └── load
│       └── load.py
├── README.md
├── dags
│   └── nyc_dag.py
├── dashboard
│   └── app.py
├── dbt_project
│   ├── analyses
│   ├── dbt_project.yml
│   ├── macros
│   │   ├── generate_staging_yaml.sql
│   │   └── get_describe_payment_type.sql
│   ├── models
│   │   ├── core
│   │   │   ├── dim_zone.sql
│   │   │   ├── dm_hourly_operation.sql
│   │   │   ├── dm_monthly_zone.sql
│   │   │   ├── dm_origin_destination.sql
│   │   │   ├── dm_tipping.sql
│   │   │   ├── fact_trips.sql
│   │   │   └── schema.yml
│   │   └── staging
│   │       ├── source_schema.yml
│   │       ├── stg_green_data.sql
│   │       └── stg_yellow_data.sql
│   ├── packages.yml
│   ├── profiles.yml
│   └── seeds
│       └── taxi_zone_lookup.csv
├── docker-compose.yml
└── requirements.txt
```
## **Features**
- **Data Ingestion**: Tự động crawl trips data (Yellow & Green Taxi) từ nguồn dữ liệu công khai
- **Data Lake Storage**: Lưu trữ Raw Data trên MinIO đảm bảo khả năng mở rộng
- **Robust Transformation**: Sử dụng dbt-duckdb để biến đổi, làm sạch và kiểm thử dữ
liệu (Data Quality Testing) ngay trong quá trình xử lý
- **Concurrency Management**: Quản lý tài nguyên hiệu quả bằng Airflow 
cho phép ingest song song nhưng đảm bảo tính toàn vẹn khi ghi vào DuckDB (Single-writer lock)
- **Analytical Data Models**: Xây dựng các bảng phân tích chuyên sâu (Core Layer) 
phục vụ Business Intelligence
- **Auto Documentation & Visualization**: Tự động sinh tài liệu mô tả ý nghĩa các tables và 
features dictionary kết hợp dashboard bằng Streamlit để theo dõi chỉ số kinh doanh
- **Containerized Deployment**: Toàn bộ môi trường từ Build Tools, Orchestration đến Storage đều được đóng gói bằng Docker
## **Prerequisites**
- Docker và Docker Compose
- Python 3.8+
- Airflow 2.10.5

## **Technical Architecture**
Sử dụng quy trình ELT kết hợp kiến trúc MEDALLION (Bronze - Silver - Gold) 
để quản lý data pipeline :

- Data Collection (Extract): Thực hiện crawl dữ liệu định dạng Parquet từ website NYC TLC

- Data Staging (Load - Bronze Layer): Raw data được đẩy trực tiếp lên MinIO để lưu trữ lâu dài và làm source để truy cập (nếu cần)

- Data Transformation (Silver & Gold Layer):
  - Sử dụng dbt-duckdb để kết nối trực tiếp với MinIO và thực hiện transform
  - Silver Layer (Staging): Chuẩn hóa và biến đổi data. Schema và các rules kiểm thử  được định nghĩa trong `schema.yml`.
  - Gold Layer (Core): Tạo các bảng Data Mart phục vụ mục đích phân tích cụ thể:
    - Phân tích luồng di chuyển: Xác định các cặp điểm đón-trả khách phổ biến nhất (Origin-Destination)
    - Phân tích hiệu suất vận hành: Tính toán số chuyến, tốc độ trung bình theo khung giờ trong ngày
    - Phân tích hành vi Tips: Đánh giá mức độ tip của khách hàng dựa trên loại hình thanh toán và quãng đường.
  - Documentation: dbt tự động generate document mô tả chi tiết các bảng và lineage của dữ liệu.

- Orchestration & Management: Apache Airflow điều phối toàn bộ pipeline theo trình tự: Ingest -> Load -> Transform.

- Concurrency Control: Sử dụng cơ chế Pool của Airflow để xử lý bài toán khóa (Lock) của DuckDB:
  
  - Task Ingest/Load cho Yellow và Green Taxi chạy song song (Parallel)
    
  - Task dbt Transform (ghi vào DuckDB) được xếp hàng đợi (Queue) để chạy tuần tự, tránh lỗi "Database Locked"
- Export View: Sau khi xử lý xong, file DuckDB (.duckdb) được export ra volume chia sẻ để Streamlit có thể đọc (Read-only)

- Visualization: Streamlit đọc dữ liệu từ file DuckDB đã xử lý để vẽ các biểu đồ thể hiện 
xu hướng doanh thu theo tháng, mật độ di chuyển

## **Usage**
1. Khởi động hệ thống:
- docker compose up -d --build 
2. Truy cập MinIO Console (Kiểm tra Raw Data):
- URL: localhost:9001
- User: minio / Pass: minio123
3. Truy cập Airflow UI
- URL: localhost:8080
- User: admin / Pass: admin
- Run DAGs:
  - Nếu muốn backfill thì run 2 task nyc_taxi_green_ingest & nyc_taxi_yellow_ingest
    (đợi chạy xong) -> nyc_taxi_dbt_transform
  - Nếu chạy bình thường thì chỉ cần run 3 task cùng lúc
4. Truy cập Streamlit Dashboard 
- URL: localhost:8501
5. Xem Data Documentation
- URL: localhost:8081