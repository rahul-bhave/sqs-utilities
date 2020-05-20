"""
This file will contains:
a) Method to read from a queue
b) Method to parse the message from sqs queue and break when you find a required message (for now assume you are looking for a key chain in a nested dictionary)
f) Use your 'send' message code to send messages to each queue and show the output recognizes which queue received which message

"""
import boto3
import json
import logging
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.sqs_utilities_conf as conf

class sqsutility():
    # class for all sqs utility methods
    def get_messages_from_queue(self,queue_url):
        """
        Generates messages from an SQS queue.
        Note: this continues to generate messages until the queue is empty.
        Every message on the queue will be deleted.
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """
        sqs_client = boto3.client('sqs')
        print(f'Reading the message from "{queue_url}"')
        queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
        response = queue.receive_messages(QueueUrl=queue.url, AttributeNames=['All'])
        if response != []:
            responses = set()
            for response in response:
                responses.add(response.body)
                print(responses)

                return responses
        elif response == []:
            print(f'No messages in the queue "{queue_url}"')

    def send_message_to_queue(queue_url, DelaySeconds=10, MessageAttributes={}, MessageBody=()):
        """
        """
        sqs_client = boto3.client('sqs')
        response = sqs_client.send_message_to_queue(QueueName=queue_url, wait=DelaySeconds, MessageAttributes=MessageAttributes, MessageBody=MessageBody)
        print(response.get('MessageId'))

if __name__=='__main__':
    print("Start of %s"%__file__)
    #Creating an instance of the class
    sqsutility_obj = sqsutility()
    #1 sample usage of connect to sqs queue and get message from sqs queue
    for every_queue_url in conf.QUEUE_URL_LIST:
        sqsutility_obj.get_messages_from_queue(every_queue_url)
else:
        print('ERROR: Received incorrect comand line input arguments')