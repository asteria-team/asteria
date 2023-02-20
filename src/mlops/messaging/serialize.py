"""
Serialization and Deserialization tools for MLOPS library
"""

import json
from typing import Any, Dict

ENCODER = "utf-8"

# -----------------------------------------------
# JSON serial/deserialization
# -----------------------------------------------


def _json_serializer(msg_to_serial: Dict[str, Any]) -> str:
    """
    Serialize a dictionary into a json string

    :param msg_to_serial: the message to be serialized
    :type msg_to_serial: Dict[str, Any]

    :return: serialized JSON str
    :rtype: str
    """
    if msg_to_serial is None or msg_to_serial == "":
        return json.dumps({"no_message": None}).encode((ENCODER))
    else:
        return json.dumps(msg_to_serial).encode(ENCODER)


def _json_deserializer(serial_msg: bytearray) -> Dict[str, Any]:
    """
    Deserialize a json bytearray into a dictionary

    :param msg_to_serial: serialized JSON bytearray
    :type msg_to_serial: bytearray

    :return: a python object
    :rtype: Dict[str, Any]
    """
    try:
        if serial_msg is None:
            return None
        else:
            return json.loads(serial_msg.decode(ENCODER))
    except json.decoder.JSONDecodeError:
        raise RuntimeError("Error deserializing message")
