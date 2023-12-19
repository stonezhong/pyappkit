from typing import Any, Optional

import logging
logger = logging.getLogger(__name__)

import json
from pyappkit.rabbitmq import Serializer, MessageEnvelope

class MySerializer(Serializer):
    def serialize(self, obj:MessageEnvelope)->bytes:
        if type(obj) is not MessageEnvelope:
            raise Exception("Bad type")
        if type(obj.message) is not Message:
            raise Exception("Bad type")

        json_payload = obj.to_dict()
        json_payload["message"] = obj.message.to_dict()
        return json.dumps(json_payload).encode("utf-8")

    def deserialize(self, payload: bytes)->MessageEnvelope:
        json_payload = json.loads(payload.decode("utf-8"))
        message_envelope = MessageEnvelope.from_dict(json_payload)
        message_envelope.message = Message.from_dict(json_payload["message"])
        return message_envelope

class Message:
    x:int
    y:int

    def __init__(self, *, x:int, y:int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Message(x={self.x}, y={self.y})"

    def to_dict(self)->Any:
        return {
            "x": self.x,
            "y": self.y
        }

    @classmethod
    def from_dict(cls, json_payload:Any)->"Message":
        return Message(**json_payload)

