"""
h2/origin
~~~~~~~~~

Implementation of the ORIGIN frame defined by `RFC 8336
<https://www.rfc-editor.org/rfc/rfc8336>`_, along with validation of the
origins it carries.

hyperframe does not natively implement the ORIGIN frame, so h2 provides its
own frame class here and registers it with hyperframe's frame registry.
"""
from __future__ import annotations

import re
import struct
from typing import TYPE_CHECKING, Any

from hyperframe.exceptions import InvalidDataError
from hyperframe.frame import FRAMES, Frame

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

_STRUCT_H = struct.Struct(">H")

#: The type byte of the ORIGIN frame, from RFC 8336 Section 2.1.
ORIGIN_FRAME_TYPE = 0x0C

#: The longest origin that can be carried in an ORIGIN frame, limited by the
#: 16-bit Origin-Len field.
MAX_ORIGIN_LENGTH = 0xFFFF

# Matches the ASCII serialization of an origin, as defined by RFC 6454
# Section 6.2: scheme "://" host [ ":" port ]. Because this is a bytes
# pattern, anything containing non-ASCII bytes cannot match.
_ORIGIN_REGEX = re.compile(
    rb"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)"
    rb"://"
    rb"(?P<host>\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._~!$&'()*+,;=-]+)"
    rb"(?::(?P<port>[0-9]+))?",
)

#: The highest valid TCP port number.
_MAX_PORT = 65535


def _origin_is_valid(origin: bytes) -> bool:
    """
    Check whether ``origin`` is a valid ASCII serialization of an origin, as
    defined by RFC 6454 Section 6.2 and carried by the ORIGIN frame.
    """
    match = _ORIGIN_REGEX.fullmatch(origin)
    if match is None:
        return False

    port = match.group("port")
    return port is None or int(port) <= _MAX_PORT


class OriginFrame(Frame):
    """
    The ORIGIN frame is used by a server to declare which origins it believes
    this connection to be authoritative for. It is defined in `RFC 8336
    <https://www.rfc-editor.org/rfc/rfc8336>`_.

    The ORIGIN frame defines no flags and is always sent on stream 0. Its
    payload is a sequence of length-prefixed, ASCII-serialized origins.

    .. versionadded:: 4.5.0
    """

    type = ORIGIN_FRAME_TYPE

    def __init__(self, stream_id: int = 0, origins: Iterable[bytes] = (), **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The origins carried by this frame, as bytestrings.
        self.origins = list(origins)

    def _body_repr(self) -> str:
        return f"origins={self.origins!r}"

    def serialize_body(self) -> bytes:
        return b"".join(
            _STRUCT_H.pack(len(origin)) + origin for origin in self.origins
        )

    def parse_body(self, data: memoryview) -> None:
        origins = []
        position = 0
        while position < len(data):
            if len(data) - position < _STRUCT_H.size:
                msg = "Invalid ORIGIN frame body: truncated origin length."
                raise InvalidDataError(msg)
            (origin_len,) = _STRUCT_H.unpack(data[position:position + _STRUCT_H.size])
            position += _STRUCT_H.size
            if len(data) - position < origin_len:
                msg = "Invalid ORIGIN frame body: truncated origin."
                raise InvalidDataError(msg)
            origins.append(data[position:position + origin_len].tobytes())
            position += origin_len

        self.origins = origins
        self.body_len = len(data)


# hyperframe does not natively know about the ORIGIN frame, so teach its
# frame registry to produce OriginFrame objects when parsing frame headers.
FRAMES[ORIGIN_FRAME_TYPE] = OriginFrame
