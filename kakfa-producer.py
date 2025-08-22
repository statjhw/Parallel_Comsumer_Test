import requests
import json
import time
import signal
import sys
from kafka import KafkaProducer


class BaseCollector:
    def __init__(self, name: str, description: str, url: str, api_key: str = None):
        self.name = name
        self.description = description
        self.url = url 
        self.api_key = api_key

    def pretty_print(self,data):
        print(json.dumps(data, indent=2, ensure_ascii=False))

    def collect(self) -> dict :
        pass

    def fetch_all(self) -> dict:
        pass

class BinanceCollector(BaseCollector):
    def __init__(self):
        super().__init__(
            name="Binance",
            description="Binance is a cryptocurrency exchange.",
            url="https://api.binance.com/api/v3/ticker/price",
        )
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.running = True
        
    def stop(self):
        self.running = False
    def collect(self) -> dict:
        try:
            response = requests.get(self.url, timeout=10)
            timestamp = int(round(time.time() * 1000))
            if response.status_code == 200:
                return json.loads(response.text), timestamp
            else:
                raise Exception(f"API 호출 실패: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Binance API 오류: {e}")
            return None
        
    def run(self) -> dict:
        try:
            message_count = 0
            while self.running:
                tickers = {} 
                result = self.collect()
                if result is None:
                    time.sleep(3)
                    continue
                
                data, timestamp = result
                for item in data:
                    tickers[item['symbol']] = {
                        "exchange": "binance",
                        "symbol": item['symbol'],
                        "price": item['price'],
                        "timestamp": timestamp
                    }
                    key = item['symbol']
                    value = tickers[item['symbol']]
                    self.producer.send(
                        topic="binance",  # Consumer와 동일한 토픽 사용
                        key=key,
                        value=value
                    )
                    message_count += 1
                
                self.producer.flush()
                print(f"Sent {len(data)} messages to Kafka. Total: {message_count}")
                
                if not self.running:
                    break
                    
                time.sleep(3)
            
            print(f"Producer stopped. Total messages sent: {message_count}")
            return message_count

        except KeyboardInterrupt:
            print("\nReceived interrupt signal, shutting down...")
            self.running = False
        except Exception as e:
            print(f"Binance 데이터 처리 오류: {e}")
            return None
        finally:
            try:
                self.producer.flush()
                self.producer.close()
                print("Producer closed successfully.")
            except Exception as e:
                print(f"Error closing producer: {e}")
    
if __name__ == "__main__":
    collector = BinanceCollector()
    
    def signal_handler(sig, frame):
        print(f"\nReceived signal {sig}, stopping producer...")
        collector.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    collector.run()
