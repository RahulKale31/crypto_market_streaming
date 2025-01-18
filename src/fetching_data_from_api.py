import websocket
import json
from kafka import KafkaProducer
import logging
from datetime import datetime

class StockDataPipeline:
    def __init__(self, finnhub_token, kafka_bootstrap_servers=['localhost:9092']):
        self.finnhub_token = finnhub_token
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.symbols = [
            "BINANCE:BTCUSDT",    # Bitcoin
            "BINANCE:ETHUSDT",    # Ethereum
            "BINANCE:SOLUSDT",    # Solana
            "BINANCE:MATICUSDT",  # Polygon
            "BINANCE:ADAUSDT"     # Cardano
        ]
        self.kafka_topic = "stock-data"
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            # Skip ping messages
            if data.get('type') == 'ping':
                return
                
            # Process only trade data
            if data.get('type') == 'trade':
                # Extract symbol from the trade data
                symbol = data.get('data', [{}])[0].get('s', '')
                
                # Add timestamp
                data['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka with symbol as key
                self.kafka_producer.send(
                    self.kafka_topic,
                    key=symbol,
                    value=data
                ).get(timeout=10)
                
                self.logger.info(f"Trade data sent to Kafka for symbol {symbol}: {data}")
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"WebSocket Connection Closed - Status code: {close_status_code}, Message: {close_msg}")

    def on_open(self, ws):
        self.logger.info("WebSocket Connection Opened")
        # Subscribe to trade data for each symbol
        for symbol in self.symbols:
            subscribe_message = {
                "type": "subscribe",
                "symbol": symbol
            }
            ws.send(json.dumps(subscribe_message))
            self.logger.info(f"Subscribed to trades for {symbol}")
    
    def websocket_creation(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={self.finnhub_token}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        self.ws = ws  # Store ws instance for cleanup
        return ws

    def start(self):
        self.logger.info("Starting WebSocket connection...")
        ws = self.websocket_creation()
        ws.run_forever()

    def cleanup(self):
        if hasattr(self, 'kafka_producer') and self.kafka_producer:
            self.kafka_producer.close()
            self.logger.info("Kafka producer closed")

    def stop(self):
        if hasattr(self, 'ws') and self.ws:
            self.ws.close()
            self.logger.info("WebSocket connection closed")
        self.cleanup()