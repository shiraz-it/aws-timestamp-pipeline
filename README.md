# aws-timestamp-pipeline
An AWS scheduled pipeline for practice:

1. EventBridge (CloudWatch Events) send scheduled event- a trigger every 3 minutes.

2. A Poducer Lambda send message with timestampe to fifo SQS:

{
 timestamp: "12.12.20 14:47:05"
}

3. The SQS triggering Worker Lambda which read the message, than store as json object in S3 bucket. 

4. If the message contain exact hour, an email notification sent to SNS.
