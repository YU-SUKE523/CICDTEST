FROM python:3.11-slim

# 作業ディレクトリの設定
WORKDIR /app

# 必要なパッケージのインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードのコピー
COPY app.py .

# ヘルスチェック用 (オプション)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import boto3; boto3.client('sqs')" || exit 1

# アプリケーションの実行
CMD ["python", "-u", "app.py"]
