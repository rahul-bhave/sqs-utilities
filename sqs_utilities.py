"""
This file will contains:
a) Method to read from a queue
b) Method to parse the message from sqs queue and break when you find a required message (for now assume you are looking for a key chain in a nested dictionary)
c) Have a conf with a list of queues in a pipeline (list of strings)
d) As a dirty hack, use acyncio (we did a GD on this with you) to monitor each queue and then call
e) To test, you will need to setup 3 queues via the browser (first-queue, first-failure-queue, second-queue)
f) Use your 'send' message code to send messages to each queue and show the output recognizes which queue received which message

"""
import argparse
import boto3
import json
import logging

def get_messages_from_queue(queue_url):
    """
    Generates messages from an SQS queue.
    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.
    :return: The AWS response
    """
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    sqs_client = boto3.client('sqs')
    _logger.info(f'Reading the message from "{queue_url}"')
    queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
    response = queue.receive_messages(QueueUrl=queue.url, AttributeNames=['All'])
    _logger.debug(f'Triggered queue_url response: "{response}"')
    if response != []:
        responses = set()
        for response in response:
            responses.add(response.body)
            print(responses)

            return responses
    elif response == []:
            print("No messages in the queue")

def send_message_to_queue(queue_url, DelaySeconds=10, MessageAttributes={}, MessageBody=()):
    """
    sends message to the sqs queue
    :param:queue_url URL of the sqs queue where message needs to be send
    :param:DelaySeconds time set to 10 seconds is delay second
    :param:MessageAttribute you can give message attribute here
    :param:MessageBody message body can be given here
    :return:The AWS response MessageId
    """
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    sqs_client = boto3.client('sqs')
    _logger.info(f'Sending message to "{queue_url}"')
    response = sqs_client.send_message_to_queue(QueueName=queue_url, wait=DelaySeconds, MessageAttributes=MessageAttributes, MessageBody=MessageBody)
    print(response.get('MessageId'))

if __name__ == '__main__':
    #1 sample usage of connect to sqs queue and get message from sqs queue
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue_url", required=True, help="Queue URL")
    args = ap.parse_args()
    get_messages_from_queue(args.queue_url)

    """
    #2 sample usage of trigger cron job lambda
    ap = argparse.ArgumentParser()
    ap.add_argument("--lambda_name", required=True, help="lambda name")
    args = ap.parse_args()
    trigger_cron_lambda(args.lambda_name)

    #3 sample usage of send message to sqs queue
    ap = argparse.ArgumentParser()
    MessageAttributes={
        'Title': {
            'DataType': 'String',
            'StringValue': 'The Whistler'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'John Grisham'
        },
        'WeeksOn': {
            'DataType': 'Number',
            'StringValue': '6'
        }
    }
    MessageBody=(
        'Information about current NY Times fiction bestseller for '
        'week of 12/11/2016.'
    )
    ap.add_argument("--queue_url", required=True, help="Queue URL")
    ap.add_argument("--MessageAttributes", action=store, required=True, type=dict, default=MessageAttributes,help="MessageAttribute")
    ap.add_argument("--MessageBody", action=store, required=True, type=tuple, default=MessageBody, help="MessageBody" )
    args = ap.parse_args()
    send_message_to_queue(args.queue_url, args.MessageAttributes, args.MessageBody, DelaySeconds=10)
    """
