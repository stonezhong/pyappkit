# Package you need to import if you use rabbotmq module
```
pika
```

# Serializer
The system does not know how to serializ, deserialize messages, it rely on a serializer to serialize and deserialize messages

# Features
There are 3 message queues:
- Normal queue, message gets posted to this queue first, and MQProcessor.handle_message will process this message.
- Retry queue, if handle_message throws exception, the message will be put in retry queue
    - this is a message queue that bound to `delayed-exchange`. You can call MQClient.enable_delayed_exchange to enable delayed delivery for retry queue
    - There is a maxium number of times we can retry -- controlled by `retry_count`. If this time is exceeded and retry is still failing, the message will be put into dead queue further investigation.
    - The message posted to the retry queue will be delayed for delivery, the time is controlled by retry_delay_seconds.
        - Typically, many intermittent error won't recover immediately, but given some time it will recover.
- Client call MQClient.send_message to post message to the normal queue


