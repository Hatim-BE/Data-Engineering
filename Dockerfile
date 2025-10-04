FROM python:3.13

# RUN apt-get install wget
RUN pip install pandas pyarrow fastparquet sqlalchemy psycopg2-binary psycopg2 psycopg[binary]

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY data/ data/

ENTRYPOINT [ "python" , "ingest_data.py"]