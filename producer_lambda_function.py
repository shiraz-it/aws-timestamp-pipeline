import json
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime


QUEUE_NAME = "QUEUE_NAME.fifo"
AWS_ACCOUNT_ID = "XXXXXXXXXXXX"


def send_sqs_message(queue_name, aws_account_id, msg_body):
    sqs_client = boto3.client('sqs')
    response = sqs_client.get_queue_url(QueueName=queue_name,
                                        QueueOwnerAWSAccountId=aws_account_id)
    sqs_queue_url = response["QueueUrl"]

    try:
        message_status = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                                 MessageBody=json.dumps(msg_body),
                                                 MessageGroupId="group_id_1")
        message_id = message_status["MessageId"]
        logging.info(f"Sent SQS message ID: {message_id}")
        return message_id

    except ClientError as error:
        logging.error(error)
        return None


def generate_message(event):
    current_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    return json.dumps({"timestamp": current_time})


def lambda_handler(event, context):
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s: %(asctime)s: %(message)s")

    message_body = generate_message(event)
    message_id = send_sqs_message(QUEUE_NAME, AWS_ACCOUNT_ID, message_body)

    return json.dumps({"MessageId": message_id,
                       "MessageBody": message_body})
