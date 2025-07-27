FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_node/ ./storage_node/

RUN mkdir -p /data

EXPOSE 8001

CMD ["python", "-m", "storage_node.agent"]
