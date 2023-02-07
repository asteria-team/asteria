## Messaging Library for MLOPS Pipeline

The messaging library for the MLOPS Pipeline defines the core message structure used in the pipeline. This is mainly used by the pipeline's underlying orchestration to pass messages in a consistent way. The MLOPS_Message class then provides a way to interact with the message prior to sending or after getting back sent messages. 

In addition to the MLOPS_Message, the library defines several Enum classes to help further standardize messaging across the pipeline.

### Library Use

The main use of this library is with the orchestration library to pass an object (the MLOPS_Message) through the underlying orchestration allowing for automatic synchronous or asynchrounous mangagment of the MLOPS pipeline.

The structure of the MLOPS_Message remains the same regardless of the underlying orchestration which allows users to write code that will work with any orchestrator. The orchestration library in the mlops library is closely tied the messaging library. It expects an `MLOPS_Message` as input and will create a genaric one if it is not provided. It also guarentees that if messages meeting the user requirements exist, they will be returned in the form of an `MLOPS_Message` or `List[MLOPS_Message]`. 

The MLOPS_Message also allows for unique user input for special requirements that need to be sent in a message. This is in the MLOPS_Message dictionary under *additional_arguments* and can be generated upon class initialization or set later with the general use *set_message_data* method.

To get any of the sections of the MLOPS_Message, use one of the get methods. Each method will return the content of that particular section or `None` if there is no data for the section. To set data, either pass key word arguments on initialization of the MLOPS_Message class or call the *set_message_data* method to set the particular section with new or additional data.

When passing the MLOPS_Message back and forth to the orchestration library, there is no need to serialize or deserialize. This will be handled by the orchestration library. The only requirements on the user is to create MLOPS_Messages and manages the return results.

### Code Examples

#### Create an MLOPS_Message

The following code shows how to make a mlops_msg with several keyword arguments passed. If `mlops_msg.get_user_message()` is called, `"Hello World"` will be returned. If `mlops_msg.get_stage()` is called, `None` is returned as that was not set.

```
import mlops.messaging as mes

user_msg = "Hello World"
mlops_msg = mes.MLOPS_Message(
    mes.Message_Type.COMPLETE.name,
    creator="ETL",
    user_message=user_msg,
    output="/dataset/data"
)
```

#### Set data in an MLOPS_Message

Using the same message we previously created, we can update the contents of the message by calling the general purpose *set_message_data* method. After the *set_message_data* is called for `"stage"` the `mlops_msg.get_stage()` call will return `mes.Annotation_Stage.STAGED`.

```
mlops_msg.set_message_data("stage", mes.Annotation_Stage.STAGED)
mlops_msg.set_message_data("additional_arguments", {"my_dict": "fizzbuzz"})
```

#### Get contents from an MLOPS_Message

The following code snippet calls the orchestration consumer method *get_messages* which returns a list of MLOPS_Messages (can be empty). For each message, the user can pull relevant information that may inform how to proceed in the pipeline.

```
msgs = con.get_messages()
for msg in msgs:
    print(msg.get_creator_type())
    print(msg.get_topic())
    print(msg.user_message())
```