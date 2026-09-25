"""
h2/priority
~~~~~~~~~~~

Implementation of the extensible prioritization scheme for HTTP, defined in
`RFC 9218 <https://www.rfc-editor.org/rfc/rfc9218>`_.

This module contains the PRIORITY_UPDATE frame (which hyperframe does not
currently provide) and helpers to parse and serialize the structured priority
field value that carries the ``urgency`` and ``incremental`` parameters.

h2 only transports this priority information: it does not act on it, and it
does not implement a network scheduler on behalf of the application.

.. versionadded:: 4.5.0
"""
from __future__ import annotations

import struct
from typing import Any

from hyperframe.exceptions import InvalidFrameError
from hyperframe.frame import FRAMES, Frame

#: The default urgency of a response, used when the priority field value does
#: not carry a usable ``u`` parameter. See RFC 9218, Section 4.1.
DEFAULT_URGENCY = 3

#: The minimum and maximum urgency values defined by RFC 9218, Section 4.1.
MIN_URGENCY = 0
MAX_URGENCY = 7

_STRUCT_L = struct.Struct("!L")


class PriorityUpdateFrame(Frame):
    """
    The PRIORITY_UPDATE frame (type 0x10), defined in RFC 9218, Section 6.

    Clients send this frame on stream 0 to (re)prioritize a request stream;
    servers send it on a push stream to (re)prioritize that pushed response.
    The payload is a 31-bit prioritized element ID followed by the priority
    field value.
    """

    #: The byte used to define the type of the frame.
    type: int = 0x10

    #: Clients send PRIORITY_UPDATE on stream 0, servers send it on the push
    #: stream, so both stream associations are possible.
    stream_association = "either"

    def __init__(self,
                 stream_id: int,
                 prioritized_element_id: int = 0,
                 field_value: bytes = b"",
                 **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The ID of the element (a stream) whose priority is being updated.
        self.prioritized_element_id = prioritized_element_id

        #: The priority field value, exactly as sent on the wire.
        self.field_value = field_value

    def _body_repr(self) -> str:
        return (
            f"prioritized_element_id={self.prioritized_element_id}, "
            f"field_value={self.field_value!r}"
        )

    def serialize_body(self) -> bytes:
        return (
            _STRUCT_L.pack(self.prioritized_element_id & 0x7FFFFFFF) +
            self.field_value
        )

    def parse_body(self, data: memoryview) -> None:
        try:
            (prioritized_element_id,) = _STRUCT_L.unpack(data[:4])
        except struct.error as err:
            # RFC 9218, Section 6: a PRIORITY_UPDATE frame with a length of
            # less than 4 octets is a connection error of type
            # FRAME_SIZE_ERROR. h2 maps InvalidFrameError onto
            # FrameDataMissingError, which carries that error code.
            msg = "Invalid PRIORITY_UPDATE data"
            raise InvalidFrameError(msg) from err

        self.prioritized_element_id = prioritized_element_id & 0x7FFFFFFF
        self.field_value = data[4:].tobytes()
        self.body_len = len(data)


# Register the frame with hyperframe so that received PRIORITY_UPDATE frames
# are parsed into this class instead of being surfaced as unknown extension
# frames.
FRAMES[PriorityUpdateFrame.type] = PriorityUpdateFrame


def parse_priority_field_value(field_value: bytes) -> tuple[int, bool]:
    """
    Parse a priority field value (as carried by the PRIORITY_UPDATE frame and
    the ``Priority`` header field) into its ``urgency`` and ``incremental``
    components.

    Unknown parameters are ignored, as required by RFC 9218, Section 4.
    Unusable values (non-ASCII data, malformed integers, out-of-range urgency)
    are also ignored, causing the defaults to be used: an urgency of 3 and
    non-incremental delivery.

    :param field_value: The serialized priority field value.
    :returns: A tuple of ``(urgency, incremental)``.
    """
    urgency = DEFAULT_URGENCY
    incremental = False

    try:
        text = field_value.decode("ascii")
    except UnicodeDecodeError:
        return urgency, incremental

    for member in text.split(","):
        key, separator, value = member.partition("=")
        key = key.strip()
        value = value.strip()

        if key == "u" and separator:
            try:
                parsed_urgency = int(value)
            except ValueError:
                continue
            if MIN_URGENCY <= parsed_urgency <= MAX_URGENCY:
                urgency = parsed_urgency
        elif key == "i":
            # A bare "i" member or "i=?1" means true; "i=?0" means false.
            incremental = not separator or value == "?1"

    return urgency, incremental


def serialize_priority_field_value(urgency: int, incremental: bool) -> bytes:
    """
    Serialize ``urgency`` and ``incremental`` into a priority field value
    suitable for the PRIORITY_UPDATE frame.

    :param urgency: The urgency of the response, an integer between 0 and 7
        inclusive (see RFC 9218, Section 4.1).
    :param incremental: Whether the response should be delivered
        incrementally (see RFC 9218, Section 4.2).
    :returns: The serialized priority field value.
    """
    if not MIN_URGENCY <= urgency <= MAX_URGENCY:
        msg = f"Urgency must be between {MIN_URGENCY} and {MAX_URGENCY}, got {urgency}"
        raise ValueError(msg)

    field_value = f"u={urgency}".encode("ascii")
    if incremental:
        field_value += b", i"
    return field_value
