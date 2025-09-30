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

symbols = indice2

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
    print(f"[{topic}] {result}")

def getError(error):
    print(f"⚠️ WebSocket lỗi: {error}")

def stream(symbol): 
    while True:
        try:
            selected_channel = f"MI:{symbol}"
            mm = MarketDataStream(config, MarketDataClient(config))
            mm.start(get_market_data, getError, selected_channel)  
        except Exception as e:
            print(f"❌ Lỗi với {symbol}, sẽ reconnect sau 1s: {e}")
            time.sleep(1)
 
        except KeyboardInterrupt:
            print("🛑 Đóng kết nối MarketDataStream...")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=50) as executor:
        for i, sym in enumerate(symbols):
            time.sleep(0.01)  # nghỉ 200ms tránh rate limit
            executor.submit(stream, sym)


