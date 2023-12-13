#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from typing import Any

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG",
    },
}
import logging.config
from typing import Type
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)

from pyappkit import AbstractSerializer, SerializerManager
import json


class Foo:
    x: int
    y: int

    def __init__(self, *, x:int, y:int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Foo(x={self.x}, y={self.y})"


class Bar(Foo):
    color: str

    def __init__(self, *, x:int, y:int, color:str):
        super().__init__(x=x, y=y)
        self.color = color

    def __repr__(self):
        return f"Bar(x={self.x}, y={self.y}, color={self.color})"

class MySerializer:
    def serialize(self, obj: Any) -> bytes:
        if type(obj) == Foo:
            return json.dumps({"_type": "Foo", "x":obj.x, "y": obj.y}).encode("utf-8")
        if type(obj) == Bar:
            return json.dumps({"_type": "Bar", "x":obj.x, "y": obj.y, "color": obj.color}).encode("utf-8")
        raise Exception("Unrecognized type")

    def deserialize(self, payload:bytes) -> Any:
        payload_json = json.loads(payload.decode("utf-8"))
        type = payload_json.pop("_type")
        print(f"type: {type}")
        if type == "Foo":
            return Foo(**payload_json)
        if type == "Bar":
            return Bar(**payload_json)
        raise Exception("Unrecognized type")


def main():
    logger.info("Blah...")
    s = MySerializer()

    obj = Bar(x=1, y=2, color='red')

    # serialize
    p = s.serialize(obj)
    print(f"Serialize: {p}")

    # deserialize
    o = s.deserialize(p)
    print(o)


if __name__ == '__main__':
    main()
