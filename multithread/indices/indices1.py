from List import config
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from concurrent.futures import ThreadPoolExecutor    
from List.indice import *
import threading

# DOCKER BUILD
# KAFKA_BROKER = '172.18.0.3:9092'

# LOCAL TEST
KAFKA_BROKER = 'localhost:9092'

symbols = ['VNINDEX']#indice1

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


def get_market_data(message):
    data = json.loads(message.get("Content","{}"))
    symbol=data['IndexId']
    symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
    symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol
    
    result = {
        'function': 'indices',
        'content': {
            'symbol': symbol,
            'point': data['IndexValue'],
            'change': data['Change'],
            'ratioChange': data['RatioChange'],
            'totalVol': data['AllQty'],
            'totalVal': data['AllValue'],
            'advancersDecliners': [
                data['Advances']+data['Ceilings'],
                data['NoChanges'],
                data['Declines']+data['Floors']
            ]
        }
    }

    # Gửi Kafka
    topic = f"indice_{symbol}"
    producer.send(topic, result)

def getError(error):
    print(f"⚠️ WebSocket lỗi: {error}")

def stream(symbol): 
    selected_channel = f"MI:{symbol}"
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



