from typing import Any, Optional

import logging
logger = logging.getLogger(__name__)

import json
from datetime import datetime, timedelta
from pyappkit.rabbitmq import Serializer, MessageEnvelope, MessageDebugInfo

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

def str2dt(dt_s:Optional[str])->Optional[datetime]:
    return None if dt_s is None else datetime.strptime(dt_s, DATETIME_FORMAT)

def dt2str(dt:Optional[datetime])->Optional[str]:
    return None if dt is None else dt.strftime(DATETIME_FORMAT)

def seconds2td(seconds:float)->Optional[timedelta]:
    return None if seconds is None else timedelta(seconds=seconds)

def td2seconds(td:Optional[timedelta])->Optional[float]:
    return None if td is None else td.total_seconds()

class MySerializer(Serializer):
    def serialize(self, obj:MessageEnvelope)->bytes:
        if type(obj) is not MessageEnvelope:
            raise Exception("Bad type")
        if type(obj.message) is not Message:
            raise Exception("Bad type")

        message = obj.message
        json_payload = {
            "queued_time": dt2str(obj.queued_time),
            "process_time": dt2str(obj.process_time),
            "process_duration": td2seconds(obj.process_duration),
            "debug_infos": [
                {
                    "queued_time": dt2str(i.queued_time),
                    "process_time": dt2str(i.process_time),
                    "process_duration": td2seconds(i.process_duration),
                    "exception_message": i.exception_message,
                    "exception_type": i.exception_type
                } for i in obj.debug_infos

            ],
            "message": {
                "x": message.x,
                "y": message.y
            }
        }
        return json.dumps(json_payload).encode("utf-8")

    def deserialize(self, payload: bytes)->MessageEnvelope:
        json_payload = json.loads(payload.decode("utf-8"))

        json_payload["message"] = Message(**json_payload['message'])
        json_payload["debug_infos"] = [
            MessageDebugInfo(
                queued_time=str2dt(i["queued_time"]),
                process_time=str2dt(i["process_time"]),
                process_duration=seconds2td(i["process_duration"])
            ) for i in json_payload["debug_infos"]
        ]


        json_payload['queued_time'] = str2dt(json_payload['queued_time'])
        json_payload['process_time'] = str2dt(json_payload['process_time'])
        json_payload['process_duration'] = seconds2td(json_payload['process_duration'])
        return MessageEnvelope(**json_payload)

class Message:
    x:int
    y:int

    def __init__(self, *, x:int, y:int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Message(x={self.x}, y={self.y})"

