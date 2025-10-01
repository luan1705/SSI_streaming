from List import config
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from concurrent.futures import ThreadPoolExecutor    
from List.exchange import DERIVATIVES
from List.indices_map import indices_map

# DOCKER BUILD
# KAFKA_BROKER = '172.18.0.3:9092'

# LOCAL TEST
KAFKA_BROKER = 'localhost:9092'

list = DERIVATIVES

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
    # exchange=find_exchange(symbol,exchange_map)
    indices=find_indice(symbol,indices_map)

    result={
            'function':'eboard_table',
            'content': {
                'symbol': symbol,
                'exchange': 'DERIVATIVES',
                'indices':indices,
                'ceiling': data['Ceiling'] / 1000,
                'floor': data['Floor'] / 1000,
                'refPrice': data['RefPrice'] / 1000,
                'buy':{
                    'price': [data['BidPrice1'] / 1000,data['BidPrice2'] / 1000,data['BidPrice3'] / 1000],
                    'vol': [data['BidVol1'] ,data['BidVol2'] ,data['BidVol3'] ]
                },
                'match':{
                    'price': data['LastPrice'] / 1000,
                    'vol': data['LastVol'],
                    'change': data['Change']/1000,
                    'ratioChange': data['RatioChange'],
                },
                'sell':{
                    'price': [data['AskPrice1'] / 1000,data['AskPrice2'] / 1000,data['AskPrice3'] / 1000],
                    'vol': [data['AskVol1'] ,data['AskVol2'] ,data['AskVol3'] ]
                },
                'totalVol': data['TotalVol'],
                'totalVal': data['TotalVal'],
                'high': data['High'] / 1000,
                'low': data['Low'] / 1000,
                'open':data['Open']/1000,
                'close':data['Close']/1000
            }

        }

    # Gửi Kafka
    topic = f"eboard_table_{data['Symbol']}"
    producer.send(topic, result)

def getError(error):
    print(f"⚠️ WebSocket lỗi: {error}")

def stream(symbol): 
    selected_channel = f"X:{symbol}"
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


