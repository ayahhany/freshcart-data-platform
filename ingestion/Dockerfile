FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY full_load.py .

CMD ["python", "-u", "full_load.py"]
