FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY . .

RUN chmod +x /app/exec/plink2 && \
    mv /app/exec/plink2 /usr/local/bin/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]