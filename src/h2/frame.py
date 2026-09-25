"""
h2/frame
~~~~~~~~

Additional frame types that are not (yet) provided by hyperframe.

Currently this contains the ORIGIN frame defined by RFC 8336.
"""
from __future__ import annotations

import re
import struct
from typing import TYPE_CHECKING

from hyperframe.exceptions import InvalidDataError
from hyperframe.frame import Frame

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

_STRUCT_H = struct.Struct(">H")

# The serialized form of an origin, as defined by RFC 6454 Section 6.2:
# scheme "://" host [ ":" port ]. The host may be a registered name, an IPv4
# address, or a bracketed IP literal (RFC 3986). Origins carried in ORIGIN
# frames are ASCII-only (RFC 8336 Section 2.1 calls the field "ASCII-Origin"),
# which this pattern enforces by only matching ASCII character classes.
_ORIGIN_RE = re.compile(
    rb"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://"
    rb"(?P<host>\[[0-9A-Fa-f:.]+\]|[^\s/?#@:\[\]]+)"
    rb"(:(?P<port>[0-9]+))?\Z",
)


def validate_origin(origin: bytes) -> bool:
    """
    Validate that a bytestring is a syntactically valid serialized origin, as
    defined by RFC 6454 Section 6.2 and required in ORIGIN frames by
    RFC 8336.

    :param origin: The serialized origin to validate.
    :returns: ``True`` if the origin is syntactically valid, else ``False``.
    """
    if not origin.isascii():
        return False

    match = _ORIGIN_RE.match(origin)
    if match is None:
        return False

    port = match["port"]
    return port is None or int(port) <= 65535


class OriginFrame(Frame):
    """
    The ORIGIN frame, defined by RFC 8336 (type 0xC).

    This frame is sent by servers on stream 0 to assert the origins for which
    the connection is authoritative. Its payload is a sequence of origin
    entries, each made of a 16-bit length followed by the ASCII serialization
    of an origin.
    """

    #: The byte used to define the type of the frame.
    type = 0xC

    def __init__(self, stream_id: int = 0,
                 origins: Iterable[bytes] = (), **kwargs) -> None:
        super().__init__(stream_id, **kwargs)

        #: The origins asserted by this frame, as serialized origins in the
        #: form defined by RFC 6454 Section 6.2.
        self.origins: list[bytes] = list(origins)

    def _body_repr(self) -> str:
        return f"origins={self.origins!r}"

    def serialize_body(self) -> bytes:
        body = bytearray()
        for origin in self.origins:
            body += _STRUCT_H.pack(len(origin))
            body += origin
        return bytes(body)

    def parse_body(self, data: memoryview) -> None:
        self.origins = []

        offset = 0
        while offset < len(data):
            if len(data) - offset < _STRUCT_H.size:
                msg = "ORIGIN frame with truncated origin length"
                raise InvalidDataError(msg)

            (origin_len,) = _STRUCT_H.unpack(data[offset:offset + _STRUCT_H.size])
            offset += _STRUCT_H.size

            if len(data) - offset < origin_len:
                msg = "ORIGIN frame with truncated origin"
                raise InvalidDataError(msg)

            origin = bytes(data[offset:offset + origin_len])
            offset += origin_len

            if not validate_origin(origin):
                msg = f"ORIGIN frame with invalid origin: {origin!r}"
                raise InvalidDataError(msg)

            self.origins.append(origin)

        self.body_len = len(data)
