import boto3
from datetime import datetime
import json
import time
import uuid
import random
import string
import sys
import traceback

# 標準出力のバッファリングを無効化
sys.stdout.reconfigure(line_buffering=True)

client = boto3.client('sns', region_name='ap-northeast-1')

# アラート用SNSトピックARN
ALERT_TOPIC_ARN = 'arn:aws:sns:ap-northeast-1:977614337881:sns-u13-ficc-test-sales1-alert-topic'

def logging(errorLv, app_name, errorMsg):
    loggingDateStr = (datetime.now()).strftime('%Y%m%d %H:%M:%S')
    message = f"{loggingDateStr} {app_name} [{errorLv}] {errorMsg}"
    print(message, flush=True)
    return

def generate_random_string(length=10):
    """ランダムな文字列を生成"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def send_alert_notification(message_id, counter, message_content):
    """アラート用SNSトピックに成功通知を送信"""
    app_name = "ECS-SNS-Publisher"
    
    try:
        alert_subject = f"[SUCCESS] SNS Message Published - #{counter}"
        alert_body = f"""
SNSメッセージのPublishが成功しました。

【詳細情報】
- 送信時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- メッセージ番号: #{counter}
- MessageId: {message_id}
- 送信先トピック: arn:aws:sns:ap-northeast-1:977614337881:sns-u13-ficc-test-trade-book-topic.fifo

【送信内容】
{message_content}

このメッセージは自動送信されています。
"""
        
        alert_params = {
            'TopicArn': ALERT_TOPIC_ARN,
            'Subject': alert_subject,
            'Message': alert_body
        }
        
        alert_response = client.publish(**alert_params)
        logging("WARN", app_name, f"Alert notification sent. MessageId: {alert_response.get('MessageId')}")
        return alert_response
        
    except Exception as e:
        logging("ERROR", app_name, f"Failed to send alert notification: {str(e)}")
        logging("ERROR", app_name, f"Traceback: {traceback.format_exc()}")
        # アラート送信失敗は致命的エラーとしない

def publish_to_sns(message):
    app_name = "ECS-SNS-Publisher"
    
    logging("ERROR", app_name, "Publishing message to SNS")
    
    message_group_id = f"group-{generate_random_string(8)}"
    message_deduplication_id = str(uuid.uuid4())
    
    params = {
        'TopicArn': 'arn:aws:sns:ap-northeast-1:977614337881:sns-u13-ficc-test-trade-book-topic.fifo',
        'Subject': 'Published From: ECS-Fargate-Test',
        'Message': message,
        'MessageGroupId': message_group_id,
        'MessageDeduplicationId': message_deduplication_id
    }
    
    logging("WARN", app_name, f"MessageGroupId: {message_group_id}")
    logging("WARN", app_name, f"MessageDeduplicationId: {message_deduplication_id}")
    
    try:
        response = client.publish(**params)
        logging("WARN", app_name, f"Response: {json.dumps(response, default=str)}")
        return response
    except Exception as e:
        logging("ERROR", app_name, f"Failed to publish: {str(e)}")
        logging("ERROR", app_name, f"Traceback: {traceback.format_exc()}")
        raise

def main():
    app_name = "ECS-SNS-Publisher"
    logging("WARN", app_name, "=== Container started ===")
    logging("WARN", app_name, f"Python version: {sys.version}")
    logging("WARN", app_name, f"Boto3 version: {boto3.__version__}")
    
    counter = 0
    while True:
        try:
            counter += 1
            message = f"Test message #{counter} from ECS Fargate at {datetime.now().isoformat()}"
            
            logging("WARN", app_name, f"Attempting to publish message #{counter}")
            response = publish_to_sns(message)
            logging("WARN", app_name, f"Successfully published message #{counter}")
            
            # Publish成功後、アラート通知を送信
            message_id = response.get('MessageId', 'N/A')
            send_alert_notification(message_id, counter, message)
            
        except Exception as e:
            logging("ERROR", app_name, f"Error in main loop: {str(e)}")
            logging("ERROR", app_name, f"Traceback: {traceback.format_exc()}")
        
        logging("WARN", app_name, "Waiting 2 minutes until next publish...")
        time.sleep(120)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging("WARN", "ECS-SNS-Publisher", "Container stopped by user")
    except Exception as e:
        logging("ERROR", "ECS-SNS-Publisher", f"Fatal error: {str(e)}")
        logging("ERROR", "ECS-SNS-Publisher", f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
