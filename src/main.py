from fetching_data_from_api import StockDataPipeline
if __name__ == "__main__":
    # Replace with your Finnhub token
    FINNHUB_TOKEN = "<enter token>"
    
    pipeline = StockDataPipeline(FINNHUB_TOKEN)
    
    try:
        pipeline.start()
    except KeyboardInterrupt:
        pipeline.cleanup()
        print("Pipeline stopped by user")
    except Exception as e:
        print(f"Pipeline error: {e}")
        pipeline.cleanup()