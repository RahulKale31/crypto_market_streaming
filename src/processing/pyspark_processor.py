from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType, LongType
from src.utils.config import ConfigLoader
from src.utils.logger import Logger

class SparkStreamProcessor:
    def __init__(self):
        # Initialize logger
        self.logger = Logger(__name__, 'spark_processor.log').get_logger()
        
        # Load configuration
        self.config = ConfigLoader()
        self.kafka_config = self.config.get_kafka_config()
        self.postgres_config = self.config.get_postgres_config()
        self.spark_config = self.config.get_spark_config()
        
        # Updated schema to exactly match Finnhub's message format
        self.trade_schema = StructType([
            StructField("data", ArrayType(StructType([
                StructField("c", StringType(), True),     
                StructField("p", DoubleType(), True),     # Price
                StructField("s", StringType(), True),     # Symbol
                StructField("t", LongType(), True),       # Timestamp
                StructField("v", DoubleType(), True),     # Volume
            ])), True)
        ])

    def create_spark_session(self):
        return (SparkSession.builder
                .appName(self.spark_config['app_name'])
                .config("spark.sql.streaming.checkpointLocation", self.spark_config['checkpoint_location'])
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.23")
                .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
                #HDFSBackedStateStoreProvider stores state data in HDFS-compatible file systems (including local filesystem)
                .config("spark.sql.streaming.stateStore.maintenance.cleanupDelay", "1800s")
                #Sets how long (1800s = 30 minutes) to wait before cleaning up old state checkpoints
                .getOrCreate())

    def write_to_postgres(self, batch_df, batch_id):
        try:
            self.logger.info(f"Processing batch ID: {batch_id}")
            self.logger.info("Sample data from batch:")
            batch_df.show(5, truncate=False)
            
            postgres_properties = {
                "driver": "org.postgresql.Driver",
                "url": f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}",
                "dbtable": self.postgres_config['table'],
                "user": self.postgres_config['user'],
                "password": self.postgres_config['password']
            }
            
            (batch_df.write
                .format("jdbc")
                .options(**postgres_properties)
                .mode("append")
                .save())
                
            self.logger.info(f"Successfully wrote batch {batch_id} to PostgreSQL")
                
        except Exception as e:
            self.logger.error(f"Error writing to PostgreSQL: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def process_stream(self):
        spark = self.create_spark_session()
        self.logger.info("Created Spark session")
        
        # Read from Kafka
        df = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", ",".join(self.kafka_config['bootstrap_servers']))
             .option("subscribe", self.kafka_config['topic'])
             .option("startingOffsets", "earliest")
             .load())
        
        # Parse the JSON and explode the data array
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.trade_schema).alias("json_data")
        ).select(
            explode(col("json_data.data")).alias("trade")
        )
        
        # Select individual fields from the trade data
        trade_df = parsed_df.select(
            col("trade.s").alias("symbol"),
            col("trade.p").alias("price"),
            col("trade.v").alias("volume"),
            (col("trade.t") / 1000).cast("timestamp").alias("timestamp")
        )
        
        # Calculate moving averages
        window_avg = (trade_df
                     .withWatermark("timestamp", self.spark_config['watermark_delay'])
                     .groupBy(
                         window(col("timestamp"), self.spark_config['processing_interval']),
                         col("symbol")
                     )
                     .agg({
                         "price": "avg",
                         "volume": "sum"
                     })
                     .select(
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         col("symbol"),
                         col("avg(price)").alias("avg_price"),
                         col("sum(volume)").alias("total_volume")
                     ))
        
        # Write to PostgreSQL
        query = (window_avg.writeStream
                .outputMode("append")
                .foreachBatch(self.write_to_postgres)
                .start())
        
        self.logger.info("Started streaming query")
        return query

if __name__ == "__main__":
    processor = SparkStreamProcessor()
    try:
        query = processor.process_stream()
        query.awaitTermination()
    except Exception as e:
        processor.logger.error(f"Error in main process: {str(e)}")
        import traceback
        processor.logger.error(traceback.format_exc())