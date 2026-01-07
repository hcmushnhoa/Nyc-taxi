import requests
import boto3
from airflow.decorators import task
s3_endpoint = "http://minio:9000"
s3_access = "minio"
s3_secret = "minio123"
def get_minio_client(s3_endpoint,s3_access,s3_secret):
    """Khởi tạo MinIO Client"""
    return boto3.client('s3',
                        endpoint_url=s3_endpoint, # Hostname trong Docker Network
                        aws_access_key_id=s3_access,
                        aws_secret_access_key=s3_secret,
                        region_name="us-east-1"
                        )
@task(task_id="ingest_bronze")
def ingest_to_minio_bronze(taxi_type:str, year:str , month:str):
    file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    bucket_name = "taxi-data"
    object_key = f"{taxi_type}/{year}/{month}/{file_name}"
    print(f"--- [Bronze] Streaming Ingestion: {year}-{month} ---")
    s3_client = get_minio_client(s3_endpoint,s3_access,s3_secret)
  # 3. Tạo Bucket nếu chưa có
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        print(f"Bucket {bucket_name} chưa tồn tại. Đang tạo...")
        s3_client.create_bucket(Bucket=bucket_name)

    # 4. Streaming Download -> Upload (Direct Pipeline)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        with requests.get(url, stream=True, headers=headers) as r:
            r.raise_for_status() # Báo lỗi nếu link chết (404) hoặc server lỗi (500)

            print(f"Đang stream data từ {url} lên MinIO...")

            # upload_fileobj đọc trực tiếp từ stream của requests (r.raw)
            # Không tốn RAM, không tốn Disk
            s3_client.upload_fileobj(
                Fileobj=r.raw,
                Bucket=bucket_name,
                Key=object_key
            )

        print(f"--- Success: Uploaded to s3://{bucket_name}/{object_key} ---")
        return f"s3://{bucket_name}/{object_key}"

    except Exception as e:
        print(f"Lỗi Ingestion: {e}")
        raise e