FROM python:3.14-slim

WORKDIR /app

# Copia primeiro o requirements para aproveitar cache de build
COPY requirements.txt .


RUN pip install --upgrade pip
RUN pip install "psycopg[binary,pool]" 
RUN pip install --no-cache-dir -r requirements.txt

COPY /plugins/ingestion/ingestion.py .

CMD ["sh", "-c", "python ingestion.py"]