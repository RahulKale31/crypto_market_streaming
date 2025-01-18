import yaml
import os
from dotenv import load_dotenv

class ConfigLoader:
    def __init__(self, config_path="config/config.yaml"):
        # Load environment variables from .env file
        load_dotenv()
        
        # Load config file
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Override with environment variables if they exist
        self._override_from_env()
    
    def _override_from_env(self):
        """Override configuration with environment variables."""
        # API configuration
        if os.getenv('FINNHUB_TOKEN'):
            self.config['api']['finnhub']['token'] = os.getenv('FINNHUB_TOKEN')
        
        # PostgreSQL configuration
        if os.getenv('POSTGRES_PASSWORD'):
            self.config['postgres']['password'] = os.getenv('POSTGRES_PASSWORD')
        if os.getenv('POSTGRES_USER'):
            self.config['postgres']['user'] = os.getenv('POSTGRES_USER')
        
        # Kafka configuration
        if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
            self.config['kafka']['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')

    def get_config(self):
        """Return the complete configuration."""
        return self.config
    
    def get_finnhub_config(self):
        """Return Finnhub-specific configuration."""
        return self.config['api']['finnhub']
    
    def get_kafka_config(self):
        """Return Kafka-specific configuration."""
        return self.config['kafka']
    
    def get_postgres_config(self):
        """Return PostgreSQL-specific configuration."""
        return self.config['postgres']
    
    def get_spark_config(self):
        """Return Spark-specific configuration."""
        return self.config['spark']