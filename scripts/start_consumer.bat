@echo off
echo Starting Spark streaming consumer...
cd ..
set PYTHONPATH=%CD%
call venv_3.9\Scripts\activate
python src/processing/pyspark_processor.py