import os

class Config:
    """
    Create configuration class for single point of truth
    """

    ### API KEY ###
    try:
        with open("/workspaces/realtime-stock-market-de-project/credential/finnhub_api.json", "r") as f:
            api_key = json.load(f)
            EXTERNAL_API_KEY = api_key.get("FINNHUB_API_KEY")
            if EXTERNAL_API_KEY:
                print("Successfully retreived api key")
    except Exception as e:
        print("Couldn't not load or find api key")


    #### KAFKA ####

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
    KAFKA_TOPIC_PRICES = "stock_prices"
    KAFKA_CLIENT_ID = "stock-ingestion-producer"

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_USER = os.getenv("MINIO_USER", "")
    MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "")
    MINIO_BUCKET_RAW = "raw-stock-data"
    MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() in ("true", "1", "t")


settings = Config()