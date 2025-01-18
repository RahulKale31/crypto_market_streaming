from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType, LongType
import logging
import traceback

class SparkStreamProcessor:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
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
                .appName("CryptoDataProcessor")
                .config("spark.sql.streaming.checkpointLocation", "checkpoint")
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.23")
                .getOrCreate())

    def write_to_postgres(self, batch_df, batch_id):
        try:
            self.logger.info(f"Processing batch ID: {batch_id}")          
            self.logger.info("Sample data from batch:")
            batch_df.show(5, truncate=False)
            
            postgres_properties = {
                "driver": "org.postgresql.Driver",
                "url": "jdbc:postgresql://localhost:5432/stockdb",
                "dbtable": "stock_metrics",
                "user": "<enter_user_name>",
                "password": "<enter_password>"
            }
            
            (batch_df.write
                .format("jdbc")
                .options(**postgres_properties)
                .mode("append")
                .save())
                
        except Exception as e:
            self.logger.error(f"Error writing to PostgreSQL: {str(e)}")            
            self.logger.error(traceback.format_exc())

    def process_stream(self):
        spark = self.create_spark_session()
        self.logger.info("Created Spark session")
        
        # Read from Kafka
        df = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
             .option("subscribe", self.kafka_topic)
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
            (col("trade.t") / 1000).cast("timestamp").alias("timestamp")  # Convert milliseconds to timestamp
        )
        
        # Debug: Show the trade data
        self.logger.info("Showing trade data schema:")
        trade_df.printSchema()
        
        # Calculate 5-minute moving averages with explicit window fields
        window_avg = (trade_df
                     .withWatermark("timestamp", "10 minutes")
                     .groupBy(
                         window(col("timestamp"), "5 minutes"),
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
    processor = SparkStreamProcessor(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="stock-data"
    )
    try:
        query = processor.process_stream()
        query.awaitTermination()
    except Exception as e:
        processor.logger.error(f"Error in main process: {str(e)}")