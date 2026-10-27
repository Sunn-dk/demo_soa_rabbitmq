FROM python:3.12-slim
WORKDIR /lab
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py broker.py store.py worker.py producer.py kafka_lab.py worker2.py index.html ./
COPY static/lab.css static/lab.js ./static/
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
