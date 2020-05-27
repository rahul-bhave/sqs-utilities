"""
This file will contain:
a) Method to read from a queue using asyncio.
b) Method to send the mesage to queue asyncio.
c) Method send messages to each queue and show the output recognizes which queue received which message

"""
import asyncio
from asyncio import Future
import boto3
import json
import logging
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.sqs_utilities_conf as conf
import conf.aws_configuration_conf as aws_conf
from pythonjsonlogger import jsonlogger
from sqs_listener.daemon import Daemon
from sqs_listener import SqsListener

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

    async def get_messages_from_queue(self,future,queue_url):
        """
        Generates messages from an SQS queue.
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """
        _logger = logging.getLogger(__name__)
        _logger.setLevel(logging.DEBUG)
        await future
        sqs_client = boto3.client('sqs')
        self.logger.info(f'Reading the message from "{queue_url}"')
        queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
        response = queue.receive_messages(QueueUrl=queue.url, AttributeNames=['All'])
        if response != []:
            responses = set()
            for response in response:
                responses.add(response.body)
                self.logger.info(responses)
                return responses
        elif response == []:
            self.logger.info(f'No messages in the queue "{queue_url}"')


    async def send_message_to_queue(self,future,queue_url):
        """
        Sends message to specific queue
        :param queue_url: URL of the SQS queue to drain.
        :return: The AWS response
        """
        _logger = logging.getLogger(__name__)
        _logger.setLevel(logging.DEBUG)
        await asyncio.sleep(3)
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
        future.done()
        future.set_result("future is resolved")

async def main():
    """
    Schedule calls concurrently
    # https://www.educative.io/blog/python-concurrency-making-sense-of-asyncio
    """
    future = Future()
    sqsmessage_obj = sqsmessage()
    loop = asyncio.get_event_loop()

    t1 = loop.create_task(sqsmessage_obj.send_message_to_queue(future,'first-queue'))
    t2 = loop.create_task(sqsmessage_obj.get_messages_from_queue(future,'first-queue'))
    t3 = loop.create_task(sqsmessage_obj.send_message_to_queue(future,'second-queue'))
    t4 = loop.create_task(sqsmessage_obj.get_messages_from_queue(future,'second-queue'))
    t5 = loop.create_task(sqsmessage_obj.send_message_to_queue(future,'first-failure-queue'))
    t6 = loop.create_task(sqsmessage_obj.get_messages_from_queue(future,'first-failure-queue'))

    await t6, t5, t4, t3, t2, t1

if __name__=='__main__':
    #Running asyncio main
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    # asyncio.run(main())
else:
    print('ERROR: Received incorrect comand line input arguments')