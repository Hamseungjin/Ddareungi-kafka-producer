import json
import requests
from confluent_kafka import Producer
import time
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Kafka 서버 및 토픽 설정
# Kafka 브로커:포트
servers = ['kafka_node1:9092', 'kafka_node2:9092', 'kafka_node3:9092']
topicName = 'bike-station-info' # 사용할 Kafka 토픽 이름

# Kafka Producer 설정
conf = {'bootstrap.servers': ','.join(servers)}
producer = Producer(**conf)

api_key_files = [
    "/home/ubuntu/api_key/api_key1.bin",
    "/home/ubuntu/api_key/api_key2.bin",
    "/home/ubuntu/api_key/api_key3.bin",
    "/home/ubuntu/api_key/api_key4.bin"
]

def load_api_keys(api_key_files):
    api_keys = []
    for file_path in api_key_files:
        with open(file_path, "r", encoding="UTF-8") as api_key_file:
            api_keys.append(api_key_file.read().strip())
    return api_keys

api_keys = load_api_keys(api_key_files)
api_key_index = 0

def request_seoul_api(seoul_api_key, start_index, end_index):
    logging.info(f"Requesting data from Seoul API for range {start_index} to {end_index}")
    api_server = f'http://openapi.seoul.go.kr:8088/{seoul_api_key}/json/bikeList/{start_index}/{end_index}'
    response = requests.get(api_server)
    if response.status_code == 200:
        logging.info(f"Received data successfully for range {start_index} to {end_index}")
    else:
        logging.error(f"Failed to receive data: {response.status_code} - {response.text}")
    return response

messages_sent = 0
messages_delivered = 0

def delivery_report(err, msg):
    global messages_delivered
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        messages_delivered += 1
        if messages_delivered == messages_sent:
            logging.info(f"All {messages_sent} messages delivered successfully")

def send_data():
    global messages_sent, messages_delivered, api_key_index
    logging.info("Starting data transmission loop")
    while True:
        try:
            bike_stations = []
            seoul_api_key = api_keys[api_key_index]
            for start_index in range(1, 2001, 1000):
                end_index = start_index + 999
                response = request_seoul_api(seoul_api_key, start_index, end_index)
                if response.status_code == 200:
                    bike_stations.extend(response.json()['rentBikeStatus']['row'])
            
            messages_sent = 0
            messages_delivered = 0
            for station in bike_stations:
                data = {
                    "rackTotCnt" : station['rackTotCnt'],
                    "stationName" : station['stationName'],
                    "parkingBikeTotCnt" : station['parkingBikeTotCnt'],
                    "shared" : station['shared'],
                    "stationLatitude" : station['stationLatitude'],
                    "stationLongitude": station['stationLongitude'],
                    "stationId" : station['stationId'],
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                station_id = station['stationId']
                producer.produce(topic=topicName,
                                key=str(station_id),
                                value=json.dumps(data, ensure_ascii=False),
                                callback=delivery_report)
                producer.poll(0)
                
                logging.info(f"Sent data to Kafka: {data}")
                
                messages_sent += 1

            producer.flush()

            api_key_index = (api_key_index + 1) % len(api_keys)
            logging.info(f"Sleeping for 30 seconds before next cycle")
            time.sleep(30)

        except Exception as e:
            logging.error(f"Error: {e}")

def main():
    send_data()

if __name__ == "__main__":
    main()