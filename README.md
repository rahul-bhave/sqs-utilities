# sqs-utilities:

_What are the contents of the repository ?_:

a) sqs_messages.py: This file contains following two methods:
_a.1_: Method to read from sqs queue
_a.2_: Method to send message to sqs queue

b) conf/sqs_utilities_conf.py file: This will list out all the sqs queues first-queue, first-failure-queue, second-queue

c) templates/sample_message.json: This template will be used to send message to the sqs queue.

d) sqs_listner.py: 

_What will be done in sqs_listner.py: ?_:

a) Currently using `nohup python utils/sqs_listner.py stop > listener.log` messages can be polled in the log file.
Following message will be shown in the listner.log file
```
{"message": "Found credentials in environment variables."}
{"message": "Listening to queue first-queue"}
{"message": "1 messages received"}
```
b) You can stop the message using following command, the message will be cleaned up.
`nohup python utils/sqs_listner.py stop> listener.log &`

e) There is also `multi_sqs_listner.py` which is WIP:(This section will be updated once the work is over on this'



