## Orchestration Library for the MLOPS Pipeline

The orchestration library for the MLOPS Pipeline acts as a wrapper layer to allow the user to run the same code regardless of the underlying orchestration infrastructure. This library exposes three classes: Producer, Consumer, Admin. The Producer allows the user to send messages, the Consumer alloes the user to recieve messages, and the Admin allows the user to get orchestration related metadata. ***Currently there is no support for the Admin class. Updates will be coming soon.***

### Library Use

If the user wants to utilize underlying orchestration in the MLOPS pipeline, the orchestration library provides a way to do that with code that is independent of the orchestrator. The purpose of using the orchestration is to allow the pipeline to operate asynchronously and without human intervention except when absolutely necessary. The use of the Producer and Consumer allows separate phases of the pipeline to automatically start upon reciept of particular messages.

If connections cannot be made or no underlying orchestration exists, calls to the Producer and Consumer will not fail. Instead, logging alerts will occur. This is to prevent orchstration errors from causing the whole pipeline to fail.

#### Producer

The Producer is used to send messages to the orchestrator. It can be called without any user input and will automatically detect the underlying orchestrator in that case and connect to it. If the orchestrator is not in a normally accessed location (for example Kafka is generally located at `"kafka:9092"` in the context of a docker or docker-compose) the user must pass the name and endpoint of the orchestrator in order to allow the Producer to connect and send messages. In addition, the user can pass topics the Producer should write messages to and additional key word arguments that can be passed to the underlying orchestration specific function to better control the behavior of the Producer.

The Producer has three methods: *send*, *flush*, and *close*. Send will push messages to the orchestrator. If specific message content needs to be passed, the user needs to pass an MLOPS_Message. If the desire is to run the producer completely synchronous, the user can set the flush flag to `True`. However, note that this will cause the Producer to block until it confirms that the message(s) have been sent and propogated to the orchestrator and will cause problems if more messages need to be sent. Without flush, messages will send asynronously and the Producer can continue to send messages normally. 

The flush method does the same task that a `True` flush flag in send does, but it can be used when the Producer is complete with all of its messages and the user wants to ensure all messages propgate. This is unnecessary in most cases.

The close method will diconnect the Producer from the orchestrator and can be called once the user is done with the Producer completely.


#### Consumer

The Consumer is used to send messages to the orchestrator. It can be called without any user input and will automatically detect the underlying orchestrator in that case and connect to it. If the orchestrator is not in a normally accessed location (for example Kafka is generally located at `"kafka:9092"` in the context of a docker or docker-compose) the user must pass the name and endpoint of the orchestrator in order to allow the Consumer to connect and send messages. In addition, the user can pass topics the Consumer should subscribe to and additional key word arguments that can be passed to the underlying orchestration specific function to better control the behavior of the Consumer.

The Consumer has five methods: *subscribe*, *unsubscribe*, *get_subscriptions*, *get_messages*, and *close*. As with the Producer, close will disconnect the Consumer from the orchestrator and should only be called when the user is completely done with the consumer. 

The three subscribe methods help the user manage what topic(s) the consumer will pull from when getting messages. Subscribe allows the user to add or override where the Consumer pulls messages from. Note that subscriptions add after initialization my impact what messages the Consumer gets. Unsubscribe removes all subscriptions for the Consumer. Until a new subscription is added, the Consumer will be unable to get messages. The final method allows a user to see a list of what the consumer is currently subscribed to. No subscriptions return an empty list.

The final method *get_messages* is how the Consumer pulls messages from the orchestrator. It takes a callable filter that defaults to `True` (in other words all messages are returned). The filter can help reduce the number of messages being returned by only returning relevant data. The filter should use the MLOPS_Message class methods to interact with the messages. 

#### Use with Kafka

One orchestrator option is Kafka. All Producer and Consumer methods can be used with Kafka. The kafka-python library is being used under the hood. This is important to know if the user wants to adjust the default behaviors of the Producers and Consumers. More information about keyword arguments that can be passed can be found at [KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html) and [KafkaConsumer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html). Note that during initialization for the Producer `bootstrap_servers` and `value_serializer` are already passed. For Consumer initialization `bootstrap_servers` and `value_deserializer` are passed as well as subscription if provided.

The following are some commonly used keyword arguments for kafka:

Producer
    - *acks*: sets when the producer returns from a send with success
    - *max_block_ms*: the maximum amount of time to block during send
    - *max_request_size*: the maximum size (bytes) of a request to send (single message or in batch)
    - *api_version*: The version of Kafka in use. This helps with connection and may change some default settings and functionalities

Consumer:
    - *group_id*: sets the group the Consumer is a part of. Will change how the Consumer consumes messages in the topic if multiple Consumers are in the group
    - *auto_offset_reset*: sets the policy for where in the offset the Consumer starts reading from. If set to "earliest" the Consumer will start at the earliest available (beginning) of the logs.
    - *max_poll_records*: The maximum number of records to return in a single poll call. Note that this limit is prior to any additional filters that may be added to the Consumer at a later point. 
    - *api_version*: The version of Kafka in use. This helps with connection and may change some default settings and functionalities
Further, if Kafka is the orchestrator, it is assumed that it is located at `"kafka:9092"`. If this is not the case, the orchestrator and endpoint need to passed to both the Producer and Consumer in order to connect.

Finally, the Consumer utilizes the method *poll*. This means the Consumer tries to pull messages that are available starting at the last offset it read from (or that the group it is in read from). Thus, in order to get regular updates, it is recommended that this call should be in while loop that will remain `True` until the Consumer (or pipeline) is complete with all work. Additionally, note that *poll* does run some set up the first time it is called within the context of a Consumer which may cause it to return nothing even if messages exist. Additional calls will allow it to find and read those messages.

### Code Examples

#### Producer

The following code snippet initializes Producers and sends a message to topics in an orchestrator

```
import mlops.orchestration as orc

# Producer determines the orchestrator
new_prod = orc.Producer(topic="Cool-New-Topic")

# Producer gets orchestrator passed with multiple topics and additional keyword arguments
new_prod2 = orc.Producer(
        orchestrator_endpoint="kafka:9092"
        orchestrator="kafka",
        topic=["Cool-New-Topic", "Another-Topic"]
        acks="all",
        max_request_size=20000
    )

# Send a message. Note mlops_msg is an instance of the MLOPS_Message class
new_prod.send(mlops_msg)
new_prod2.send(mlops_msg)
```
#### Consumer

The following code snippet initializes a Consumer and reads messages from the topic it is subscribed to from the orchestrator.

```
import mlops.orchestration as orc

# Consumer determines orchestrator with additional keyword arguments
new_con = orc.Consumer(topic="Cool-New-Topic", auto_offset_reset="earliest")

# Consumer gets orchestrator passed with multiple topics to subscribe to
new_con2 = orc.Consumer(
        "kafka:9092",
        "kafka",
        topic = ["Cool-New-Topic", "some-other-topic"]
    )

# subscribe to another topic
new_con2.subscribe("a-3rdTopic")

# Loop and recieve messages
while True:
    msgs = new_con2.get_messages(filter=lambda msg: msg.get_topic() == "Cool-New-Topic")
    # Check if any messages have been recieved
    if msgs != []:
        # Some messages were recieved
        # Do something

    time.sleep(10)
    # try getting messages again
```