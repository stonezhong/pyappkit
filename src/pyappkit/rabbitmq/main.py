import logging
logger = logging.getLogger(__name__)

from typing import Tuple, Any, Callable, Optional, List
from abc import ABC, abstractmethod
from datetime import timedelta, datetime

from pyappkit import dt2str, td2num, str2dt, num2td

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel

DEFAULT_RABBITMQ_PORT = 5672
DEFAULT_HEARTBEAT = 3600*5     # 5 hours

class MessageDebugInfo:
    queued_time: datetime                   # when this message is queued
    process_time: Optional[datetime]        # when we start to process this message
    process_duration: Optional[timedelta]   # how long we took to process this message
    exception_message: Optional[str]        # exception message
    exception_type: Optional[str]           # exception class name


    def __init__(
        self,
        *,
        queued_time: datetime,
        process_time: Optional[datetime] = None,
        process_duration: Optional[timedelta] = None,
        exception_message: Optional[str] = None,
        exception_type: Optional[str] = None
    ):
        self.queued_time = queued_time
        self.process_time = process_time
        self.process_duration = process_duration
        self.exception_message = exception_message
        self.exception_type = exception_type

    def __repr__(self):
        return f"MessageDebugInfo(queued_time='{self.queued_time}', process_time='{self.process_time}', process_duration='{self.process_duration}', exception_message={self.exception_message}, exception_type={self.exception_type})"

    def to_dict(self)->Any:
        return {
            "queued_time": dt2str(self.queued_time),
            "process_time": dt2str(self.process_time),
            "process_duration": td2num(self.process_duration),
            "exception_message": self.exception_message,
            "exception_type": self.exception_type
        }

    @classmethod
    def from_dict(cls, json_payload:Any)->"MessageDebugInfo":
        return MessageDebugInfo(
            queued_time = str2dt(json_payload["queued_time"]),
            process_time = str2dt(json_payload.get("process_time")),
            process_duration = num2td(json_payload.get("process_duration")),
            exception_message = json_payload.get("exception_message"),
            exception_type = json_payload.get("exception_type"),
        )

class MessageEnvelope:
    queued_time: Optional[datetime]                         # when this message is queued
    process_time: Optional[datetime]                        # when we start to process this message
    process_duration: Optional[timedelta]                   # how long does it took to process this message
    debug_infos: List[MessageDebugInfo]                     # past process debug info
    message: Any                                            # message payload

    def __init__(
        self,
        *,
        message:Any,
        process_time:Optional[datetime]=None,
        process_duration:Optional[timedelta]=None,
        queued_time:Optional[datetime]=None,
        debug_infos:Optional[List[MessageDebugInfo]]=list()
    ):
        self.queued_time = queued_time
        self.process_time = process_time
        self.process_duration = process_duration
        self.debug_infos = debug_infos
        self.message = message

    def push_error(self, e:Exception):
        message_debug_info = MessageDebugInfo(
            queued_time=self.queued_time,
            process_time=self.process_time,
            process_duration=self.process_duration,
            exception_message = str(e),
            exception_type = type(e).__name__
        )
        self.debug_infos.append(message_debug_info)
        self.queued_time = None
        self.process_time = None
        self.process_duration = None

    def __repr__(self):
        return f"MessageEnvelope(queued_time='{self.queued_time}', process_time='{self.process_time}', process_duration='{self.process_duration}', message={self.message}, debug_infos={[debug_info for debug_info in self.debug_infos]})"

    def to_dict(self)->Any:
        return {
            "queued_time": dt2str(self.queued_time),
            "process_time": dt2str(self.process_time),
            "process_duration": td2num(self.process_duration),
            "debug_infos": [debug_info.to_dict() for debug_info in self.debug_infos],
            "message": None
        }
    
    @classmethod
    def from_dict(cls, json_payload:Any)->"MessageEnvelope":
        return MessageEnvelope(
            queued_time = str2dt(json_payload["queued_time"]),
            process_time = str2dt(json_payload.get("process_time")),
            process_duration = num2td(json_payload.get("process_duration")),
            debug_infos = [
                MessageDebugInfo.from_dict(debug_info_payload) for debug_info_payload in json_payload["debug_infos"]
            ],
            message = None
        )


class Serializer:
    @abstractmethod
    def serialize(self, message_envelope:MessageEnvelope)->bytes:
        pass

    def deserialize(self, payload:bytes)->MessageEnvelope:
        pass

class MQClient:
    username: str
    password: str
    hostname: str
    port: int

    def __init__(self, *, username:str, password:str, hostname:str, port:int=DEFAULT_RABBITMQ_PORT):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port

    def create_queue(self, queue_name:str)->None :
        # create a queue
        connection, channel = self.get_channel()
        with connection:
            with channel:
                channel.queue_declare(queue_name, durable=True, arguments={"x-queue-type": "classic"})


    def enable_delayed_exchange(self, queue_name:str)->None:
        connection, channel = self.get_channel()
        with connection:
            with channel:
                channel.exchange_declare(
                    "delayed-exchange",
                    "x-delayed-message",
                    durable=True,
                    arguments={"x-delayed-type": "direct"}
                )
                channel.queue_bind(exchange='delayed-exchange', queue=queue_name)

    def get_channel(self, heartbeat:int=DEFAULT_HEARTBEAT)->Tuple[BlockingConnection, BlockingChannel]:
        credentials = pika.PlainCredentials(self.username, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.hostname,
            port=self.port,
            credentials=credentials,
            heartbeat=heartbeat
        ))
        channel = connection.channel()
        return connection, channel


    def send_bytes(self, *, queue_name:str, channel:BlockingChannel, body: bytes, delay_seconds:Optional[int]=None)->None:
        if delay_seconds is None:
            prop = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=body,
                properties=prop
            )
            return

        prop = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            headers = {"x-delay": delay_seconds*1000}
        )
        # You need to setup delayed-exchange and bind your queue to it
        channel.basic_publish(
            exchange='delayed-exchange',
            routing_key=queue_name,
            body=body,
            properties=prop
        )

    def send_message(
        self,
        *,
        queue_name:str,
        channel:BlockingChannel,
        message:Any,
        serialize:Serializer,
        debug_infos:List[MessageDebugInfo]=list(),
        delay_seconds:Optional[int]=None
    )->None:
       message_envelope = MessageEnvelope(
           message=message,
           debug_infos=debug_infos,
           queued_time=datetime.utcnow()
        )
       self.send_bytes(
           queue_name=queue_name,
           channel=channel,
           body=serialize.serialize(message_envelope),
           delay_seconds=delay_seconds
        )

###############################################################################################################
# Pull message from message queue and handle each message
###############################################################################################################
class MQProcessor(ABC):
    mq_client: MQClient
    serializer:Serializer
    inactivity_timeout:timedelta        # how long we need to be blocked if there is no message
    queue_name:str                      # name of the queue we are processing
    retry_queue_name:str                # if we fail to process the message, the message will be placed here for retry
    failed_queue_name:str               # if retry failed, the message will be placed here
    retry_count:int                     # how many time we should retry before giving up?
    retry_delay_seconds:Optional[int]   # how long do we need to wait before retry?
    prefetch_count:int                  # prefetch count, when we are pulling the message queue
    quit_requested: Callable[[], bool]  # a callback so we know if quit has been requested

    def __init__(
        self,
        *,
        mq_client:MQClient,
        serializer:Serializer,
        queue_name:str,
        retry_queue_name:str,
        failed_queue_name:str,
        retry_count:int,
        quit_requested:Callable[[], bool],
        inactivity_timeout=timedelta(seconds=60),
        prefetch_count:int=1,
        retry_delay_seconds:Optional[int]=None
    ):
        self.mq_client = mq_client
        self.serializer = serializer
        self.queue_name = queue_name
        self.retry_queue_name = retry_queue_name
        self.failed_queue_name = failed_queue_name
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds
        self.quit_requested = quit_requested
        self.inactivity_timeout = inactivity_timeout
        self.prefetch_count = prefetch_count


    @abstractmethod
    def handle_message(self, message_envelope:MessageEnvelope)->None:
       # This is called when we receive a message and pass to this method to process it
       pass

    def handle_error(self, channel:BlockingConnection, message_envelope:MessageEnvelope) -> None:
        logger.info(f"handle_error: got message envelope: {message_envelope}")
        retry = False
        if len(message_envelope.debug_infos) <= self.retry_count:
            queue_name=self.retry_queue_name
            retry = True
            logger.warn(f"handle_error: send to retry queue {queue_name} for retry!")
        else:
            queue_name=self.failed_queue_name
            retry = False
            logger.warn(f"handle_error: send to error queue {queue_name} and no more retries!")

        # No delay if we are sending to failed queue
        self.mq_client.send_message(
            queue_name=queue_name,
            channel=channel,
            message=message_envelope.message,
            serialize=self.serializer,
            debug_infos=message_envelope.debug_infos,
            delay_seconds=self.retry_delay_seconds if retry else None
        )

    def process_messages(self, use_retry_queue=False, message_count:Optional[int]=None)->None:
        # process messages in a loop until quit is requested
        queue_name = self.retry_queue_name if use_retry_queue else self.queue_name
        log_prefix = "MQProcessor.process_messages"
        logger.info(f"{log_prefix}: enter")
        logger.info(f"{log_prefix}: queue_name=\"{queue_name}\", use_retry_queue={use_retry_queue}, prefetch_count={self.prefetch_count}")

        processed_message_count = 0
        while not self.quit_requested() and (message_count is None or processed_message_count < message_count):
            connection, channel = self.mq_client.get_channel()
            logger.info(f"{log_prefix}: channel connected")
            with connection:
                with channel:
                    channel.basic_qos(prefetch_count=self.prefetch_count)

                    for method_frame, properties, body in channel.consume(
                        queue_name,
                        auto_ack=False,
                        exclusive=False,
                        inactivity_timeout=self.inactivity_timeout.total_seconds()
                    ):
                        if self.quit_requested():
                            # we've been asked to quit
                            # since we didn't acked, the message will not be removed from queue
                            logger.info(f"{log_prefix}: quit has been requested, bail out the message loop!")
                            break
                        if method_frame is None:
                            # no message
                            continue

                        try:
                            message_envelope = self.serializer.deserialize(body)
                        except:
                            logger.warning(f"{log_prefix}: cannot open envelope from message payload: {body}")
                            continue

                        if not isinstance(message_envelope, MessageEnvelope):
                            logger.warning(f"{log_prefix}: cannot open envelope from message payload: {body}, {type(message_envelope)}: not a message envelope!")
                            continue

                        handled = False
                        try:
                            message_envelope.process_time = datetime.utcnow()
                            self.handle_message(is_retry=use_retry_queue, message_envelope=message_envelope)
                            message_envelope.process_duration = datetime.utcnow() - message_envelope.process_time
                            handled = True
                        except Exception as e:
                            message_envelope.process_duration = datetime.utcnow() - message_envelope.process_time
                            message_envelope.push_error(e)
                            logger.exception(f"{log_prefix}: handle_message throws exception")
                        finally:
                            if channel.is_closed:
                                logger.info(f"{log_prefix}: channel closed, bypass ack")
                                break


                            # no matter message_handler throws exception or not, we send back ack
                            channel.basic_ack(method_frame.delivery_tag)


                            if not handled:
                                self.handle_error(channel, message_envelope)


                        processed_message_count += 1
                        if message_count is not None and processed_message_count >= message_count:
                            logger.info(f"{log_prefix}: message_count has been reached, bail out!")
                            break


                    if not channel.is_closed:
                        requeued_messages = channel.cancel() # cancel the generator
        logger.info(f"{log_prefix}: exit")
