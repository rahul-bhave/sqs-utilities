"""
This file will contain:
a) Method to read from a queue based on Message Attribute.
b) Method to send the mesage to queue

"""
import argparse
import boto3
import json
import logging
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.sqs_utilities_conf as conf
import conf.aws_configuration_conf as aws_conf
from pythonjsonlogger import jsonlogger

# logging
log_handler = logging.StreamHandler()
log_handler.setFormatter(jsonlogger.JsonFormatter())
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

#setting environment variable
os.environ["AWS_ACCOUNT_ID"]= aws_conf.AWS_ACCOUNT_ID
os.environ['AWS_DEFAULT_REGION'] = aws_conf.AWS_DEFAULT_REGION
os.environ['AWS_ACCESS_KEY_ID'] = aws_conf.AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_conf.AWS_SECRET_ACCESS_KEY

class sqsmessage():
    # class for all sqs utility methods
    logger = logging.getLogger(__name__)

    # declaring templates directory
    template_directory = 'templates'

    def get_messages_from_queue(self,queue_url):
        """
        Generates messages from an SQS queue.
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """

        _logger = logging.getLogger(__name__)
        _logger.setLevel(logging.DEBUG)
        sqs_client = boto3.client('sqs')
        queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
        while True:
            messages = sqs_client.receive_message(QueueUrl=queue.url)
            if 'Messages' in messages:
                for message in messages['Messages']:
                    if 'Body' in message.keys():
                        my_string = json.dumps(message['Body'])
                        my_string = my_string.replace('\\n', '')
                        my_string = my_string.replace('\\', '')
                        my_string = my_string.replace(' ','')
                        print(my_string)
                        print(type(my_string))
                        my_string = json.dumps(my_string)
                        print(type(my_string))
                        my_dict = json.loads(my_string)
                        print(type(my_dict))
                        my_dict = eval(my_string)
                        print(type(my_dict))
            else:
                print('Queue is now empty')
                break


    def send_message_to_queue(self,queue_url):
        """
        Sends message to specific queue
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """
        _logger = logging.getLogger(__name__)
        _logger.setLevel(logging.DEBUG)
        current_directory = os.path.dirname(os.path.realpath(__file__))
        message_template = os.path.join(current_directory,self.template_directory,'sample_message.json')
        with open(message_template,'r') as fp:
            sample_dict = json.loads(fp.read())
        sample_message = json.dumps(sample_dict)
        sample_message = sample_message.encode('utf-8')
        sqs_client = boto3.client('sqs')
        squeue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
        response = squeue.send_message(MessageBody=json.dumps(sample_dict), MessageAttributes={})
        self.logger.info(response.get('MessageId'))

if __name__=='__main__':
    #Creating an instance of the class
    sqsmessage_obj = sqsmessage()
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue_url", required=True, help="Queue URL")
    args = ap.parse_args()
    sqsmessage_obj.get_messages_from_queue(args.queue_url)

else:
        print('ERROR: Received incorrect comand line input arguments')