import json
from kafka import KafkaProducer
from minio import Minio
from config import Config

# def get_kafka_producer() -> KafkaProducer:
#     """
#     Initializes and returns a Kafka Producer.
    
#     """
#     try:
#         producer = KafkaProducer(
#             bootstrap_servers=[settings.KAFKA_BROKER],
#             client_id=settings.KAFKA_CLIENT_ID,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             retries=5,
  
#         )
#         print("Kafka Producer initialized.")
#         return producer
    
#     except Exception as e:
#         print(f"ERROR initializing Kafka Producer: {e}")
#         return None

def get_minio_client() -> Minio:
    """
    Initializes and returns a Minio client for S3-compatible storage.
    """
    try:
        minio_client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_USER,
            secret_key=settings.MINIO_PASSWORD,
            secure=settings.MINIO_SECURE
        )
        print("✅ Minio Client initialized.")
        return minio_client
    
    except Exception as e:
        print(f"❌ ERROR initializing Minio Client: {e}")
        return None


def ensure_minio_bucket_exists(minio_client: Minio, bucket_name: str):
    """
    Checks if the Minio bucket exists and creates it if it doesn't.
    """
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Minio Bucket '{bucket_name}' created.")
        
        else:
            print(f"Minio Bucket '{bucket_name}' already exists.")
    
    except Exception as e:
        print(f"ERROR : {e}")


# Example of calling the initialization (will be used in the main script)
if __name__ == '__main__':
    kafka_prod = get_kafka_producer()
    minio_cli = get_minio_client()
    
    if minio_cli:
        ensure_minio_bucket_exists(minio_cli, settings.MINIO_BUCKET_RAW)