import boto3
import json
import logging
import os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.sqs_utilities_conf as conf
import conf.aws_configuration_conf as aws_conf
from utils.sqs_messages import sqsmessage
from pythonjsonlogger import jsonlogger
from multi_sqs_listener import QueueConfig, EventBus, MultiSQSListener

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

class MyListener(MultiSQSListener):
    def low_priority_job(self, message):
        print('Starting low priority, long job: {}'.format(message))
        time.sleep(5)
        print('Ended low priority job: {}'.format(message))
    def high_priority_job(self, message):
        print('Starting high priority, quick job: {}'.format(message))
        time.sleep(.2)
        print('Ended high priority job: {}'.format(message))
    def handle_message(self, queue, bus, priority, message):
        if bus == 'high-priority-bus':
            self.high_priority_job(message.body)
        else:
            self.low_priority_job(message.body)

low_priority_bus = EventBus('low-priority-bus', priority=1)
high_priority_bus = EventBus('high-priority-bus', priority=5)
EventBus.register_buses([low_priority_bus, high_priority_bus])

low_priority_queue = QueueConfig('low-priority-queue', low_priority_bus)
high_priority_queue = QueueConfig('high-priority-queue', high_priority_bus)
my_listener = MyListener([low_priority_queue, high_priority_queue])
my_listener.listen()

def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """
    sqs_client = boto3.client('sqs')
    queue = boto3.resource('sqs').get_queue_by_name(QueueName=queue_url)
    while True:
        response = queue.receive_messages(QueueUrl=queue.url)
        if response != []:
            print(response)
        else:
            print("No message in queue")

if __name__=='__main__':
    "testing the multi_sqs_listner"
    low_q_url = 'low-priority-queue'
    high_q_url = 'high-priority-queue'
    get_messages_from_queue(low_q_url)
    get_messages_from_queue(high_q_url)