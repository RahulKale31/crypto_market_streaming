import websocket
import json
from kafka import KafkaProducer
from datetime import datetime
from src.utils.config import ConfigLoader
from src.utils.logger import Logger

class StockDataPipeline:
    def __init__(self):
        # Initialize logger
        self.logger = Logger(__name__, 'finnhub_producer.log').get_logger()
        
        # Load configuration
        self.config = ConfigLoader()
        finnhub_config = self.config.get_finnhub_config()
        kafka_config = self.config.get_kafka_config()
        
        # Set Finnhub configuration
        self.finnhub_token = finnhub_config['token']
        self.symbols = finnhub_config['symbols']
        
        # Initialize Kafka producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_config['topic']

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
                
                self.logger.info(f"Trade data sent to Kafka for symbol {symbol}")
                self.logger.debug(f"Data: {data}")  # Detailed logging for debugging
            
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")

    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {str(error)}")

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