# sqs-utilities:

_This contains following_:

1) _Utils folder_:

 a) asyncio_sqs_messages.py: You can run this utility using python utils/asyncio_sqs_messages.py. This file contains following two methods:
_a.1_: Method to get message from the queue.
_a.2_: Method to send message to the queue.

2) _Conf folder_:
a) key_conf.py: This contains key defined for filter criteria.
b) sqs_utilities_conf.py: This contains queue url which needs to be polled.
c) aws_configuration_conf.py: User needs to add following details in the aws_configuration_conf.py file
AWS_ACCOUNT_ID = "XXXXXXXX"
AWS_DEFAULT_REGION = "us-east-2"
AWS_ACCESS_KEY_ID = "XXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY = "XXXXXXXXX"

3) _Samples folder_:
a) This contains sample_message.json file which can be used to send the sample message.

# Test Setup:

_preconditions_

1) Create predefined three SQS.
2) The SQS queues, should have pre-populated SQS messages.
