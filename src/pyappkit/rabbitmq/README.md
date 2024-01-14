# Index
* [Module Dependency](#module-dependency)
* [Classes](#classes)
    * [MQProcessor](#mqprocessor)

# Module Dependency
Put `pika` in your requiremenrts.txt:
```
pika
```

# Preparation
The rabbitmq need to have [RabbitMQ Delayed Message Plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) installed.


# Classes
## MQProcessor
This class represent a rabbitmq message queue consumer. It pulls message from message queue and process it in a loop.

You should specify 3 message queues:
* `queue_name`: the main queue name
* `retry_queue_name`: when a message failed to process, it will be placed in this queue
* `failed_queue_name`: when we give up retry on failed message, the message will be placed in this queue.

MQProcess rely on `serializer` to serialize and deserialize messages.

You call `process_messages` method to kick start loop for processing message, the loop ends in 2 cases:
* You have `message_count` set to non-Non, and the message count the loop processed has exceeded this number.
* `quit_requested` method returns `True`

User need to override method `handle_message`, to process the message.

Note, the retry queue need to bind to `delayed-exchange`, here is an example on you can create retry queue:
```
    mq_client.create_queue(retry_queue_name)
    mq_client.enable_delayed_exchange(retry_queue_name)
```

When a message queued in `retry queue`, the processing of the message is delayed, controled by parameter `retry_delay_seconds`

A message will be retied up to `retry_count` times before giving up.

## MessageEnvelope
Represent a message envelope, the field message points to the actual message.

You can see how many times a message failed to process and for what reason it failed via debug_infos field.

## MQClient
Represent a message queue client.





# Features
- Client call MQClient.send_message to post message to the normal queue


