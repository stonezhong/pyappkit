# Purpose
Shows how to publish and consums message using `RabbitMQ`

# Prepare
* You need to install rabbitmq, here is an example:
```bash
mkdir -p /mnt/DATA_DISK/rabbitmq/data
docker run \
    -d \
    -h rabbitmq \
    --name rabbitmq \
    -e RABBITMQ_DEFAULT_USER=stonezhong \
    -e RABBITMQ_DEFAULT_PASS=foobar \
    -p 15672:15672 \
    -p 5672:5672 \
    -v /mnt/DATA_DISK/rabbitmq/data:/var/lib/rabbitmq \
    rabbitmq:3-management
```
* Your rabbitmq need to have `rabbitmq_delayed_message_exchange` plugin installed
* You need to create queues, run `./prepare.py`

# Test
open 3 window, run following commands:
```bash
# in window 1:
./client.py

# in window 2:
./server

# in window 3: (error handler)
./server -ie
```
Then type numbers, like 1 2 in client, see how server gets the message
if you type 10 1, consumer will failed, you can see how it posted to retry queue and gets picked up after 5 minutes by error handler.
