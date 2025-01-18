@echo off
echo Starting Finnhub data producer...
cd ..
call venv_3.9\Scripts\activate
python src/data/finnhub_client.py