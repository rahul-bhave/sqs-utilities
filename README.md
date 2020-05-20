# sqs-utilities:

_What are the contents of the repository ?_:

a) sqs_utilities.py: This file contains following two methods:
_a.1_: Method to read from sqs queue
_a.2_: Method to send message to sqs queue

b) conf/sqs_utilities_conf.py file: This will list out all the sqs queues first-queue, first-failure-queue, second-queue

c) templates/sample_message.json: This template will be used to send message to the sqs queue.

_What will be done in sqs_utilities.py ?_:

a) All three sqs queue's will be monitored.
b) Messages will be sent to the queues.
c) Output shows, which queue receives the message.
