# sqs-utilities:

_What are the contents of the repository ?_:


a) sqs_utilities.py: This file contains following two methods:
_a.1_: Method to read from sqs queue
_a.2_: Method to send message to sqs queue
_a.2_: Method to parse sqs queue and break when required message found
b) conf/queue_list.conf file: This will list out all the sqs queues first-queue, first-failure-queue, second-queue


_What will be done in sqs_utilities.py ?_:


a) All three sqs queue's will be monitored.
b) Messages will be sent to the queues.
f) Output shows, which queue receives the message.
