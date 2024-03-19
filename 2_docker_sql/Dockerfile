FROM python:3.9

RUN apt-get update -y
RUN apt-get install wget gzip
RUN pip install pandas SQLAlchemy psycopg2

WORKDIR /app
COPY upload_data.py upload_data.py

ENTRYPOINT ["python", "upload_data.py"]