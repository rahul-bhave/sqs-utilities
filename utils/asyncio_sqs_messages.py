"""
This file will contain:
a) Method to read from a queue using asyncio.
b) Method to send the mesage to queue asyncio.
c) Methods to get sqs client, queue and dict object
d) Method to filter mesage based on the filter criteria defined.
e) Main method polls to the multiple queues (3 queues are listed in the sqs_utlities_conf.py file)

"""
import argparse
import asyncio
import boto3
import collections
import json
import logging
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.aws_configuration_conf as aws_conf
import conf.sqs_utilities_conf as conf
import conf.key_conf as key_conf
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

class Sqsmessage():
    # class for all sqs utility methods
    logger = logging.getLogger(__name__)

    def __init__(self):
        # intialise the class
        # declaring templates directory
        template_directory = 'samples'
        dictionary = {}


    async def get_messages_from_queue(self,queue_url,filter_key):
        """
        Generates messages from an SQS queue.
        :param queue_url: URL of the SQS queue to drain.
        :return: True
        """
        sqs_client = self.get_sqs_client()
        queue = self.get_sqs_queue(queue_url)
        messages = sqs_client.receive_message(QueueUrl=queue.url)

        if 'Messages' in messages:
            for message in messages['Messages']:
                self.filter_message(message,filter_key)

        return True

    async def send_message_to_queue(self,queue_url):
        """
        Sends message to specific queue
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """
        current_directory = os.path.dirname(os.path.realpath(__file__))
        message_template = os.path.join(current_directory,self.template_directory,'sample_message.json')
        with open(message_template,'r') as fp:
            sample_dict = json.loads(fp.read())
        sample_message = json.dumps(sample_dict)
        sample_message = sample_message.encode('utf-8')
        sqs_client = self.get_sqs_client()
        queue = self.get_sqs_queue(queue_url)
        response = queue.send_message(MessageBody=json.dumps(sample_dict), MessageAttributes={})
        self.logger.info(response.get('MessageId'))

    def get_dict(self,body_string):
        """
        Generates dict from message body
        :param string
        :return dict object
        """
        body_string = json.dumps(body_string)
        body_string = body_string.replace("'", "\"")
        body_string = json.loads(body_string)
        message_body_obj = json.loads(body_string)

        return message_body_obj

    def filter_message(self,message,filter_key):
        """
        Fetches filtered message from sqs queue
        :param message: message
        :return: message_body object
        """
        if 'Body' in message.keys():
            message_body_obj = self.get_dict(message['Body'])
            if filter_key in message_body_obj.keys():
                self.logger.info(message_body_obj)
            else:
                self.logger.info(f"No message polled with key {filter_key} from the queue at this moment ")
        else:
            self.logger.info("No message has body attribute")

        return True

    def get_recursive_items(self, dictionary):
        """
        This method will be used to get keys and values
        param: dict
        return : Bool
        """
        for key, value in dictionary.items():
            if type(value) is dict:
                yield (key, value)
                yield from self.get_recursive_items(value)
            else:
                yield(key,value)

        return True

    def get_dict_structure(self):
        """
        This method with dict structure of sample message json
        return: dict
        """
        current_directory = os.path.dirname(os.path.realpath(__file__))
        message_template = os.path.join(current_directory,self.template_directory,'sample_message.json')
        with open(message_template,'r') as fp:
            sample_dict = json.loads(fp.read())

        return sample_dict

    def get_sample_key_value_list(self):
        """
        getting key values from sample json
        return: key_list, value_list
        """
        sample_dict = []
        sample_dict = self.get_dict_structure()
        key_list, value_list  = self.get_value_key_list(sample_dict)

        return key_list, value_list

    def get_value_key_list(self, dictionary):
        """
        Method to get key and value list for any dict
        param: dict object
        return: key_list, value_list
        """
        key_list=[]
        value_list=[]
        for key,value in sqsmessage_obj.get_recursive_items(dictionary):
            key_list = key_list + [key]
            value_list = value_list = [value]

        return(key_list, value_list)

    def get_sqs_client(self):
        """
        Return sqs_client object
        :param none
        :return sqs_client
        """
        sqs_client = boto3.client('sqs')
        self.logger.info(sqs_client)

        return sqs_client

    def get_sqs_queue(self,queue_url):
        """
        Return queue object from queue_url
        :param queue_url
        :return queue
        """
        queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
        self.logger.info(queue)

        return queue


async def main():
    """
    Schedule calls concurrently
    # https://www.educative.io/blog/python-concurrency-making-sense-of-asyncio
    # https://www.integralist.co.uk/posts/python-asyncio/
    """
    sqsmessage_obj = Sqsmessage()
    filter_key=key_conf.filter_key
    while True:
        tasks = []
        for every_queue_url in conf.QUEUE_URL_LIST:
            tasks.append(sqsmessage_obj.get_messages_from_queue(every_queue_url,filter_key))
        result = await asyncio.gather(*tasks)

if __name__=='__main__':
    #Running asyncio main
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    asyncio.run(main())
else:
    print('ERROR: Received incorrect comand line input arguments')