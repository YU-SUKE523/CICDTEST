FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# 環境変数でPythonのバッファリングを無効化
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "app.py"]
