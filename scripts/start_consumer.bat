@echo off
echo Starting Spark streaming consumer...
cd ..
call venv_3.9\Scripts\activate
python src/processing/spark_processor.py