import boto3
import json
import logging
import os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conf.sqs_utilities_conf as conf
import conf.aws_configuration_conf as aws_conf
from pythonjsonlogger import jsonlogger
from multi_sqs_listener import QueueConfig, EventBus, MultiSQSListener
from utils.sqs_messages import sqsmessage

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
    def job_event(self, message):
        print('Starting job: {}'.format(message))
        time.sleep(2)
        print('Ended job: {}'.format(message))
    def administrative_event(self, message):
        print('Starting administrative event: {}'.format(message))
        time.sleep(.2)
        print('Ended administrative_event: {}'.format(message))
    def handle_message(self, queue, bus, priority, message):
        if bus == 'administrative-bus':
            self.administrative_event(message.body)
        else:
            self.job_event(message.body)


if __name__ == '__main__':
    # Event buses
    low_priority_job_bus = EventBus('low-priority-bus', priority=1)
    high_priority_job_bus = EventBus('high-priority-bus', priority=5)
    administrative_event_bus = EventBus('high-priority-bus', priority=10)
    EventBus.register_buses([
        low_priority_job_bus,
        high_priority_job_bus,
        administrative_event_bus
    ])

    # Queues
    low_priority_queue = QueueConfig('low-priority-queue', low_priority_job_bus)
    high_priority_queue = QueueConfig('high-priority-queue', administrative_event_bus)
    admin_queue = QueueConfig('admin-queue', administrative_event_bus)

    # Listener
    my_listener = MyListener([low_priority_queue, high_priority_queue, admin_queue])
    my_listener.listen()

    # sending messages
    sqsmessage_object = sqsmessage()
    sqsmessage.send_message_to_queue('low-priority-queue')
    sqsmessage.send_message_to_queue('low-priority-queue')
    sqsmessage.send_message_to_queue('low-priority-queue')