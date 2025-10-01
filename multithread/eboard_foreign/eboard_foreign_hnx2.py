from List import config
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from concurrent.futures import ThreadPoolExecutor    
from List.exchange import HNX2
from List.indices_map import indices_map
import threading

# DOCKER BUILD
# KAFKA_BROKER = '172.18.0.3:9092'

# LOCAL TEST
KAFKA_BROKER = 'localhost:9092'

symbols = HNX2

# Tạo Kafka producer chung, threadsafe
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1,                 # nhận ack từ leader là đủ
    linger_ms= 1,     
    batch_size= 64*1024,     
    max_in_flight_requests_per_connection=5,
    retries=0               # không retry để tránh spike độ trễ (tùy nhu cầu)
)

def find_indice(symbol: str, indices_map: dict) -> str:
    indices_list=[]
    for indices, symbols in indices_map.items():
        if symbol in symbols:
            indices_list.append(indices)
    return indices_list if indices_list else None  # không tìm thấy

def get_market_data(message):
    data = json.loads(message.get("Content","{}"))
    symbol=data['Symbol']
    result = {
        'function': 'eboard_foreign',
        'content': {
            'symbol': symbol,
            'buyVol': data['BuyVol'],
            'sellVol': data['SellVol'],
            'room': data['CurrentRoom'],
            'buyVal': data['BuyVal'] ,
            'sellVal': data['SellVal']
        }
    }

    # Gửi Kafka
    topic = f"eboard_foreign_{data['Symbol']}"
    producer.send(topic, result)
    print(f"[{topic}] {result}")

def getError(error):
    print(f"⚠️ WebSocket lỗi: {error}")

def stream(symbol): 
    selected_channel = f"R:{symbol}"
    mm = MarketDataStream(config, MarketDataClient(config))
    mm.start(get_market_data, getError, selected_channel)

def main():
    threads = []
    for sym in symbols:
        t = threading.Thread(target=stream, args=(sym,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")

if __name__ == "__main__":
	main()
