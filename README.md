# Crypto Market Real-Time Analytics Pipeline

A real-time data pipeline that processes cryptocurrency market data from Finnhub.io using Apache Kafka and PySpark.

## Architecture
```
Finnhub WebSocket → Kafka → PySpark Streaming → PostgreSQL
```

## Prerequisites
- Python 3.9
- Apache Kafka
- PostgreSQL
- Java 8 or higher (for PySpark)

## Setup

### 1. Environment Setup
```bash
# Create virtual environment
python -m venv venv_3.9
source venv_3.9/bin/activate  # On Windows: .\venv_3.9\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Finnhub API Setup
1. Sign up at https://finnhub.io/
2. Create an API Key
3. Add your API key to config/config.yaml

### 3. Kafka Setup
```bash
# Start Zookeeper
cd /path/to/kafka
bin/windows/zookeeper-server-start.bat config/zookeeper.properties

# Start Kafka
bin/windows/kafka-server-start.bat config/server.properties

# Create topic
bin/windows/kafka-topics.bat --create --topic stock-data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

### 4. PostgreSQL Setup
1. Create database and table using sql/create_tables.sql
2. Update database credentials in config/config.yaml

## Running the Pipeline

1. Start the data producer:
```bash
python src/data/finnhub_client.py
or
start_producer.bat
```

2. Start the Spark processor:
```bash
python src/processing/spark_processor.py
or
start_consumer.bat
```

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details.
