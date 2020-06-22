"""
This file will contain:
a) Method to read from a queue using asyncio.
b) Method to send the mesage to queue asyncio.
c) Methos to get sqs client, queue and dict object
d) Main method which schedules calls concurrently to deferent taks.

"""
import asyncio
import boto3
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

    # declaring templates directory
    template_directory = 'samples'

    async def get_messages_from_queue(self,queue_url):
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
                self.get_message_body(message)

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
        body_obj = json.loads(body_string)

        return body_obj

    def get_message_body(self,message):
        """
        Generates message body from message in sqs queue
        :param message: message
        :return: message_body object
        """
        if 'Body' in message.keys():
            body_obj = self.get_dict(message['Body'])
            key = key_conf.key
            key1 = key_conf.key1
            key2 = key_conf.key2
            number = key_conf.number
            if int(body_obj[key][key1][key2]) > number:
                self.logger.info(body_obj)

            return(body_obj)


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
    while True:
        tasks = []
        for every_queue_url in conf.QUEUE_URL_LIST:
            tasks.append(sqsmessage_obj.get_messages_from_queue(every_queue_url))
        result = await asyncio.gather(*tasks)

if __name__=='__main__':
    #Running asyncio main
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    asyncio.run(main())
else:
    print('ERROR: Received incorrect comand line input arguments')