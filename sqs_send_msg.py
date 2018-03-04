import boto3

sqs_client = boto3.client('sqs',aws_access_key_id='AKIAJ7HAJS5QSOC4DQZQ',aws_secret_access_key='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',region_name='us-west-2')


def send_msg(msg,sqs):
# Send message to SQS queue
    response = sqs.send_message(
    QueueUrl=queue_url,
    DelaySeconds=0,
    MessageAttributes={},
    MessageBody=(msg),
    )

    print(response)
