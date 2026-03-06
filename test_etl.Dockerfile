FROM python:3.14-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Copia apenas o arquivo de requisitos primeiro
# Isso aproveita o "cache" do Docker: se você mudar o código mas não o requirements,
# o Docker não precisa reinstalar as bibliotecas de novo.
COPY requirements.txt .

# 5. Instala as dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copia o restante do código fonte (incluindo o ingestion.py)
COPY . .

# 7. Comando para rodar o script quando o container iniciar
CMD ["python", "ingestion/ingestion.py"]
