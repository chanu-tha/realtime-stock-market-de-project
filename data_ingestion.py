from kafka import KafkaProducer
from kafka.errors import KafkaError

import time
import requests
import json

# from config.clients import get_kafka_producer, get_minio_client, ensure_minio_bucket_exists
# from config.config import Config



BASE_URL = "https://finnhub.io/api/v1/quote?"
API_KEY = settings.EXTERNAL_API_KEY

stock_mag_seven = ["AAPL", "GOOGL", "AMZN", "TSLA", "META", "MSFT", "NVDA"]

producer_config = {
    "bootstrap.servers": [settings.KAFKA_BROKER],
    "value_serializer": lambda v: json.dumps(v).encode("utf-8") 
}

## initiate connection for kafka producer
try:
    producer = KafkaProducer(producer_config)

except KafkaError as e:
    print("Failed to connect to kafka broker: {e}")

def delivery_report(err, msg):

    if err:
        print(f"{msg.key()} failed to deliver : {err}")
    else:
        #convert bit value to string
        print(f"Succesfully sent {msg.value().decode("utf-8")}")
        # to return a list of all attributes and methods for an object for inspecting
        # print(dir(msg))
        print(f"Message delivered to topic {msg.topic()} : partition {msg.partition()}: at offset {msg.offset()}")




#### DATA FETCHING ####

def fetch_stock_data(quote:str):

    url = f"{BASE_URL}symbol={quote}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["quote"] = quote

        return data
    
    except Exception as e:
        print("Error occured: {e}")

        return None
    


if __name__ == "__main__":

    try:
        while True:
            for quote in stock_mag_seven:
                
                stock_price = fetch_stock_data(quote)

                if stock_price:

                    # send message to kafka broker
                    producer.send(
                        topic= settings.KAFKA_PRICES_TOPIC,
                        key = quote.encode("utf-8"),
                        value = stock_price
                    ).add_callback(delivery_report).add_errback(delivery_report)
                    
                    print(f"Message is producing and deliver to topic: {quote}")
                
                else:
                    print(f"Skipping production for {quote}")
            
            producer.flush()
                    
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("The process is stopped by user")
        producer.close()
    
    except Exception as e:
        print(f"An un excepted error occured: {e}")
        producer.close


