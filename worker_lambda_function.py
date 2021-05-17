import json
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime

BUCKET_NAME = "S3_BUCKET_NAME"
SNS_TOPIC_ARN = "arn:aws:sns:REGION:XXXXXXXXXXXX:SNS_TOPIC_NAME"
EMAIL_SUBJECT = "Exact Hour Notification"


def put_message_in_bucket(message_body):
    object_name = message_body["timestamp"] + ".json"

    s3_client = boto3.client('s3')

    try:
        response = s3_client.put_object(Bucket=BUCKET_NAME,
                                        Key=object_name,
                                        Body=json.dumps(message_body))
        version_id = response["VersionId"]
        logging.info(f"Stored message. VersionId: {version_id}")
        return version_id

    except ClientError as error:
        logging.error(error)
        return None


def get_message_body_and_hour(event):
    message_body = event["Records"][0]["body"]
    message_body = json.loads(message_body.replace("'", "\""))
    message_hour = datetime.strptime(message_body["timestamp"][:-3], '%d/%m/%y %H:%M')
    return message_body, message_hour


def notify_sns(message_body):
    sns_client = boto3.resource('sns')
    email_content = f"Received a timestamp with exact hour. The hour is {message_body}"

    try:
        response = sns_client.publish(TopicArn=SNS_TOPIC_ARN,
                                      Subject=EMAIL_SUBJECT,
                                      Message=email_content)
        message_id = response["MessageId"]
        logging.info(f"Published message. MessageId: {message_id}")
        return message_id

    except ClientError as error:
        logging.error(error)
        return None


def lambda_handler(event, context):
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s: %(asctime)s: %(message)s")

    message_body, message_hour = get_message_body_and_hour(event)

    version_id = put_message_in_bucket(message_body)

    if message_hour.minute == 0:
        message_id = notify_sns(message_hour)
    else:
        message_id = None

    return json.dumps({"S3ObjectVersionId": version_id,
                       "SNSMessageId": message_id})
