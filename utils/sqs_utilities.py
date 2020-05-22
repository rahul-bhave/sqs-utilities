"""
This file will contain:
a) Method to read from a queue
b) Method to parse the message from sqs queue and break when you find a required message
c) Method send messages to each queue and show the output recognizes which queue received which message

"""
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

class sqsutility():
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

class MyListener(SqsListener):
    """
    Included only handle method neew to write code around it
    """
    def handle_message(self,body, attributes, MessageAttributeNames=['All']):
        return

class MyDaemon(Daemon):
    def run(self):
        print("Initializing listener")
        listener = MyListener('first-queue')
        listener.listen()

if __name__=='__main__':
    #Creating an instance of the class
    sqsutility_obj = sqsutility()
    #1 sample usage of connect to sqs queue and get message from sqs queue
    for every_queue_url in conf.QUEUE_URL_LIST:
        sqsutility_obj.get_messages_from_queue(every_queue_url)
    #2 sample usage for ssend message to sqs queue
    for every_queue_url in conf.QUEUE_URL_LIST:
        sqsutility_obj.send_message_to_queue(every_queue_url)
    """
    #3 sample example for listner
    for every_queue_url in conf.QUEUE_URL_LIST:
        listener = MyListener(every_queue_url)
        listener.listen()
    """
    daemon_obj = MyDaemon('/var/run/sqs_daemon.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            print("Starting listener daemon")
            daemon_obj.start()
        elif 'stop' == sys.argv[1]:
            print("Attempting to stop the daemon")
            daemon_obj.stop()
        elif 'restart' == sys.argv[1]:
            daemon_obj.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
else:
        self.logger.info('ERROR: Received incorrect comand line input arguments')