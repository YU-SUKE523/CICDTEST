import json
import boto3
import time
import signal
import sys
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('sns_sqs_lambda_table')
sqs_client = boto3.client("sqs")
queue_url = "https://sqs.ap-northeast-1.amazonaws.com/977614337881/sqs-u13-ficc-test-trade-book-queue.fifo"

running = True

def signal_handler(signum, frame):
    global running
    print("Shutdown signal received. Finishing current processing...")
    running = False

def process_message(message):
    try:
        body = message['Body']
        message_id = message['MessageId']
        receipt_handle = message['ReceiptHandle']
        
        print(f"Processing message: {message_id}")
        
        result = table.put_item(Item={
            'message_id': message_id,
            'subject': 'fixed subject',
            'message': body,
            'timestamp': datetime.now().isoformat(timespec='seconds')
        })
        
        print(f"✅ Successfully saved to DynamoDB: {message_id}")
        
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        print(f"✅ Deleted message from queue: {message_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error processing message: {str(e)}")
        return False

def poll_sqs():
    global running
    
    print(f"Starting to poll SQS queue: {queue_url}")
    
    poll_count = 0  # ポーリング回数をカウント
    
    while running:
        try:
            poll_count += 1
            print(f"[Poll #{poll_count}] Waiting for messages (max 20s)...")  # ← 追加
            start_time = time.time()  # ← 追加
            
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            elapsed = time.time() - start_time  # ← 追加
            messages = response.get('Messages', [])
            
            if messages:
                print(f"✅ Received {len(messages)} message(s) in {elapsed:.2f}s")  # ← 改善
                
                for message in messages:
                    if not running:
                        break
                    process_message(message)
            else:
                print(f"⏳ No messages received after {elapsed:.2f}s, continuing to poll...")  # ← 改善
                
        except Exception as e:
            print(f"❌ Error polling SQS: {str(e)}")
            time.sleep(5)
    
    print("Polling stopped")

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("Starting Fargate SQS Poller Application")
    print(f"Timestamp: {datetime.now().isoformat()}")  # ← 追加
    poll_sqs()
    print("Application shutdown complete")
