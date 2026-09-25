"""
h2/_rfc9218
~~~~~~~~~~~

Implementation of RFC 9218 (Extensible Prioritization Scheme for HTTP).

This module provides the PRIORITY_UPDATE frame (RFC 9218, Section 6) and
parsing/serialization helpers for the Priority field value, which uses the
Structured Field Dictionary syntax (RFC 8941, Section 3.2).

h2 does not act on priority information itself: it only parses it and
passes it to the application, which remains responsible for any scheduling
decisions.
"""
from __future__ import annotations

import struct
from typing import Any

from hyperframe.exceptions import InvalidFrameError
from hyperframe.frame import FRAMES, Frame

#: The frame type of the PRIORITY_UPDATE frame, as assigned by RFC 9218
#: Section 6.
PRIORITY_UPDATE_FRAME_TYPE = 0x10

#: The default urgency of a request or push, as defined by RFC 9218
#: Section 4.1.1.
DEFAULT_URGENCY = 3

_MIN_URGENCY = 0
_MAX_URGENCY = 7


class PriorityUpdateFrame(Frame):
    """
    The PRIORITY_UPDATE frame, defined by RFC 9218 Section 6.

    This frame is sent by clients on stream 0 to (re)prioritize request
    streams, and by servers on a push stream to (re)prioritize the promised
    stream. Its payload is a 31-bit prioritized element ID followed by a
    Priority field value in Structured Field Dictionary form.

    .. versionadded:: 4.5.0
    """

    #: The byte used to define the type of the frame.
    type = PRIORITY_UPDATE_FRAME_TYPE

    # Which streams this frame may be sent on depends on the role of the
    # sender, so the frame itself allows either. The role-specific rules are
    # enforced by H2Connection.
    stream_association = "either"

    def __init__(self,
                 stream_id: int,
                 prioritized_element_id: int = 0,
                 priority_field_value: bytes = b"",
                 **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The ID of the stream (or promised stream) being prioritized.
        self.prioritized_element_id = prioritized_element_id

        #: The raw Priority field value, as defined by RFC 9218 Section 4.
        self.priority_field_value = priority_field_value

    def _body_repr(self) -> str:
        return (
            f"prioritized_element_id={self.prioritized_element_id}, "
            f"priority_field_value={self.priority_field_value!r}"
        )

    def serialize_body(self) -> bytes:
        # The most significant bit of the first four octets is reserved and
        # MUST be zero when sending.
        element_id = struct.pack("!L", self.prioritized_element_id & 0x7FFFFFFF)
        return element_id + self.priority_field_value

    def parse_body(self, data: memoryview) -> None:
        # RFC 9218 Section 6: a PRIORITY_UPDATE frame with a length of less
        # than 4 octets MUST be treated as a connection error of type
        # FRAME_SIZE_ERROR. Raising InvalidFrameError here causes h2's frame
        # buffer to surface exactly that error.
        if len(data) < 4:
            msg = "Invalid PRIORITY_UPDATE frame body: too short."
            raise InvalidFrameError(msg)

        self.prioritized_element_id = struct.unpack("!L", data[:4])[0] & 0x7FFFFFFF
        self.priority_field_value = data[4:].tobytes()
        self.body_len = len(data)


# Register the frame with hyperframe so that it is parsed directly from the
# wire instead of being wrapped in a generic ExtensionFrame.
FRAMES[PriorityUpdateFrame.type] = PriorityUpdateFrame


def serialize_priority_field_value(urgency: int = DEFAULT_URGENCY, incremental: bool = False) -> bytes:
    """
    Serialize an urgency/incremental pair into a Priority field value, as
    defined by RFC 9218 Section 4. Values equal to the defaults are omitted,
    as allowed by the specification.
    """
    members = []
    if urgency != DEFAULT_URGENCY:
        members.append(f"u={urgency}")
    if incremental:
        members.append("i")
    return ", ".join(members).encode("ascii")


def parse_priority_field_value(data: bytes) -> tuple[int, bool]:
    """
    Parse a Priority field value (RFC 9218 Section 4) into an
    ``(urgency, incremental)`` pair.

    Unknown dictionary members and all parameters are ignored, as required by
    RFC 9218. Values of the wrong type, out-of-range urgencies, and field
    values that are not valid Structured Field Dictionaries are also ignored:
    in all of these cases the defaults (urgency 3, not incremental) apply.
    """
    urgency = DEFAULT_URGENCY
    incremental = False

    try:
        text = data.decode("ascii")
        members = _parse_dictionary(text)
    except (UnicodeDecodeError, ValueError):
        return urgency, incremental

    for key, value in members:
        if key == "u":
            if type(value) is int and _MIN_URGENCY <= value <= _MAX_URGENCY:
                urgency = value
        elif key == "i":
            if type(value) is bool:
                incremental = value

    return urgency, incremental


# The code below is a minimal parser for the Structured Field Dictionary
# syntax (RFC 8941, Section 3.2). It only extracts what h2 needs: dictionary
# member keys and their bare item values. Parameters are parsed only to be
# skipped over.


def _parse_dictionary(text: str) -> list[tuple[str, object]]:
    """
    Parse an RFC 8941 Dictionary. Returns a list of ``(key, bare_item)``
    tuples, where a member without an explicit value maps to ``True``.

    Raises ``ValueError`` if the input is not a valid Dictionary.
    """
    members: list[tuple[str, object]] = []
    seen_keys: set[str] = set()

    while True:
        text = text.lstrip(" \t")
        if not text:
            return members

        key, text = _parse_key(text)
        if key in seen_keys:
            msg = f"Duplicate dictionary key: {key}"
            raise ValueError(msg)
        seen_keys.add(key)

        value: object = True
        if text.startswith("="):
            value, text = _parse_bare_item(text[1:])

        text = _skip_parameters(text)
        members.append((key, value))

        text = text.lstrip(" \t")
        if not text:
            return members
        if not text.startswith(","):
            msg = "Expected comma between dictionary members"
            raise ValueError(msg)
        text = text[1:]


def _parse_key(text: str) -> tuple[str, str]:
    """
    Parse an RFC 8941 key. Returns the key and the remaining input.
    """
    if not text or not (text[0].islower() or text[0] == "*"):
        msg = "Invalid dictionary key"
        raise ValueError(msg)

    end = 0
    while end < len(text) and (text[end].islower() or text[end].isdigit() or text[end] in "_-.*"):
        end += 1
    return text[:end], text[end:]


def _skip_parameters(text: str) -> str:
    """
    Skip over any parameters (RFC 8941, Section 3.1.2) at the start of the
    input. Returns the remaining input.
    """
    while text.startswith(";"):
        text = text[1:].lstrip(" ")
        _, text = _parse_key(text)
        if text.startswith("="):
            _, text = _parse_bare_item(text[1:])
    return text


def _parse_bare_item(text: str) -> tuple[object, str]:
    """
    Parse an RFC 8941 bare item. Returns the item value and the remaining
    input. Integers are returned as ``int``, booleans as ``bool``, decimals
    as ``float``, strings and tokens as ``str``, and byte sequences as
    ``bytes``.
    """
    if not text:
        msg = "Missing bare item"
        raise ValueError(msg)

    first = text[0]
    if first == "-" or first.isdigit():
        return _parse_number(text)
    if first == '"':
        return _parse_string(text)
    if first == ":":
        return _parse_byte_sequence(text)
    if first == "?":
        return _parse_boolean(text)
    if first.isalpha() or first == "*":
        return _parse_token(text)

    msg = f"Invalid bare item: {text!r}"
    raise ValueError(msg)


def _parse_number(text: str) -> tuple[object, str]:
    """
    Parse an RFC 8941 integer or decimal.
    """
    end = 0
    if text.startswith("-"):
        end = 1

    digits_start = end
    while end < len(text) and text[end].isdigit():
        end += 1
    if end == digits_start:
        msg = "Invalid number"
        raise ValueError(msg)

    if end < len(text) and text[end] == ".":
        end += 1
        fraction_start = end
        while end < len(text) and text[end].isdigit():
            end += 1
        if end == fraction_start:
            msg = "Invalid decimal"
            raise ValueError(msg)
        return float(text[:end]), text[end:]

    return int(text[:end]), text[end:]


def _parse_string(text: str) -> tuple[str, str]:
    """
    Parse an RFC 8941 string. Returns the string content and remaining input.
    """
    end = 1
    while end < len(text):
        char = text[end]
        if char == "\\":
            end += 2
            continue
        if char == '"':
            return text[1:end].replace("\\\\", "\\").replace('\\"', '"'), text[end + 1:]
        end += 1

    msg = "Unterminated string"
    raise ValueError(msg)


def _parse_byte_sequence(text: str) -> tuple[bytes, str]:
    """
    Parse an RFC 8941 byte sequence. Returns the raw content and remaining
    input.
    """
    end = text.find(":", 1)
    if end == -1:
        msg = "Unterminated byte sequence"
        raise ValueError(msg)
    content = text[1:end]
    for char in content:
        if not (char.isalnum() or char in "+/="):
            msg = "Invalid byte sequence"
            raise ValueError(msg)
    return content.encode("ascii"), text[end + 1:]


def _parse_boolean(text: str) -> tuple[bool, str]:
    """
    Parse an RFC 8941 boolean.
    """
    if len(text) < 2 or text[1] not in "01":
        msg = "Invalid boolean"
        raise ValueError(msg)
    return text[1] == "1", text[2:]


def _parse_token(text: str) -> tuple[str, str]:
    """
    Parse an RFC 8941 token. Returns the token and the remaining input.
    """
    end = 1
    while end < len(text) and (text[end].isalnum() or text[end] in "!#$%&'*+-.^_`|~:/"):
        end += 1
    return text[:end], text[end:]
