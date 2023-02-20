## Messaging Library for MLOPS Pipeline

The messaging library for the MLOPS Pipeline defines the core message structure used in the pipeline. This is mainly used by the pipeline's underlying orchestration to pass messages in a consistent way. The MLOPSBuilder helps build an immutable message to send through the orchestrator. The MLOpsMessage class then provides a way to interact with (read) the message prior to sending or after getting back sent messages on the consumer side. 

In addition to the MLOpsMessage, the library defines several Enum classes to help further standardize messaging across the pipeline. These will continue to expand as need arises for more explanatory values.

### Library Use

The main use of this library is with the orchestration library to pass an object (the MLOpsMessage) through the underlying orchestration allowing for automatic synchronous or asynchrounous mangagment of the MLOPS pipeline.

The structure of the MLOpsMessage remains the same regardless of the underlying orchestration which allows users to write code that will work with any orchestrator. The orchestration library in the mlops library is closely tied the messaging library. It expects an `MLOpsMessage` as input. It also guarentees that if messages meeting the user requirements exist, they will be returned in the form of an `MLOpsMessage` or `List[MLOpsMessage]` when the message is recieved. 

The MLOpsMessage also allows for unique user input for special requirements that need to be sent in a message. This is in the MLOpsMessage dictionary under *additional_arguments* and can be generated upon via the build class as part of the MLOpsMessage initialization.

To get any of the sections of the MLOpsMessage, use one of the get methods called by the name of the attribute the user wants to read from. Each method will return the content of that particular attribute of the method which could be `None`. The MLOpsMessage itself is immutable. To make changes, a new message will have to be built. However, attributes can be overwritten within the MessageBuilder prior to calling the `build()` method.

When passing the MLOpsMessage back and forth to the orchestration library, there is no need to serialize or deserialize. This will be handled by the orchestration library. The only requirements on the user is to create MLOpsMessages via the MessageBuilder and manage the return results.

### Code Examples

#### Create an MLOpsMessage

The following code shows how to make a mlops_msg with several attributes with the MessageBuilder. If `mlops_msg.user_message` is called, `"Hello World"` will be returned. If `mlops_msg.stage` is called, `None` is returned as that was not set.

```
import mlops.messaging as mes

user_msg = "Hello World"
new_msg = (
        mes.MessageBuilder()
        .with_msg_type(mes.MessageType.START)
        .with_creator_type("ETL")
        .with_creator("ETL")
        .with_user_msg(test_msg)
        .with_next_task("Train")
        .build()
)
```

Once build is called, the MLOpsMessage is created and can no longer be rewritten, only read from.

#### Get contents from an MLOpsMessage

The following code snippet calls the orchestration consumer method *recieve* which returns a list of MLOpsMessages (can be empty). For each message, the user can pull relevant information that may inform how to proceed in the pipeline.

```
msgs = con.recieve()
for msg in msgs:
    # get various attributes from the message
    print(msg.creator_type)
    print(msg.topic)
    print(msg.user_message)
```