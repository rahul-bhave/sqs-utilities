"""
This file will contain:
a) Method send messages to each queue and show the output recognizes which queue received which message

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
os.environ['AWS_DEFAULT_REGION'] = aws_conf.AWS_DEFAULT_REGION
os.environ['AWS_ACCESS_KEY_ID'] = aws_conf.AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_conf.AWS_SECRET_ACCESS_KEY

class MyListener(SqsListener):
    """
    Included only handle method neew to write code around it
    """
    def handle_message(self, body, attributes, MessageAttributeNames=['Price']):
        return

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
            response = queue.receive_messages(QueueUrl=queue.url,AttributeNames=['Price'],MaxNumberOfMessages=10)
            if response != []:
                print(response)

class MyDaemon(Daemon):
    def run(self):
        print("Initializing listeners")
        listener = MyListener('first-queue')
        listener.listen()

if __name__=='__main__':
    #3 sample example for listner
    daemon_obj = MyDaemon('/var/run/sqs_daemon.pid')
    LOG_FILE = "/dev/null"
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