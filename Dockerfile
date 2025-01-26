FROM python:3.12.8


RUN apt-get update && apt-get install -y wget


RUN pip install pandas sqlalchemy psycopg2 requests 

WORKDIR /app

ENTRYPOINT ["python", "ingest_data.py"]
