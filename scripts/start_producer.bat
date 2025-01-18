@echo off
echo Starting Finnhub data producer...
cd ..
set PYTHONPATH=%CD%
call venv_3.9\Scripts\activate.bat
python src/data/finnhub_data_fetch.py