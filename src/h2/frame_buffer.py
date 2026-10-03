"""
h2/frame_buffer
~~~~~~~~~~~~~~~

A data structure that provides a way to iterate over a byte buffer in terms of
frames.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from hyperframe.exceptions import InvalidDataError, InvalidFrameError
from hyperframe.frame import ContinuationFrame, Frame, HeadersFrame, PushPromiseFrame

from .exceptions import (
    FrameDataMissingError,
    FrameTooLargeError,
    InboundBufferTooLargeError,
    ProtocolError,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._typing import Buffer

# To avoid a DOS attack based on sending loads of continuation frames, we limit
# the maximum number we're prepared to receive. In this case, we'll set the
# limit to 64, which means the largest encoded header block we can receive by
# default is 262144 bytes long, and the largest possible *at all* is 1073741760
# bytes long.
#
# This value seems reasonable for now, but in future we may want to evaluate
# making it configurable.
CONTINUATION_BACKLOG = 64


class FrameBuffer:
    """
    A buffer data structure for HTTP/2 data that allows iteration in terms of
    H2 frames.
    """

    def __init__(self, server: bool = False, max_buffer_size: int | None = None) -> None:
        self._data = bytearray()
        self.max_frame_size = 0
        self.max_buffer_size = max_buffer_size
        self._preamble = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" if server else b""
        self._preamble_len = len(self._preamble)
        self._headers_buffer: list[HeadersFrame | ContinuationFrame | PushPromiseFrame] = []

    @property
    def buffered_data_length(self) -> int:
        """
        The total number of unprocessed bytes currently retained.
        """
        return (
            len(self._data)
            + self._preamble_len
            + sum(9 + frame.body_len for frame in self._headers_buffer)
        )

    @property
    def has_complete_frame(self) -> bool:
        """
        Whether the buffer currently contains a complete frame to parse.
        """
        if self._preamble_len:
            return False
        if len(self._data) < 9:
            return False

        try:
            _, length = Frame.parse_frame_header(memoryview(self._data[:9]))
        except (InvalidDataError, InvalidFrameError) as err:  # pragma: no cover
            msg = f"Received frame with invalid header: {err!s}"
            raise ProtocolError(msg) from err

        return len(self._data) >= length + 9

    @property
    def in_header_block(self) -> bool:
        """
        Whether a HEADERS or PUSH_PROMISE block is awaiting completion.
        """
        return bool(self._headers_buffer)

    @property
    def waiting_for_preamble(self) -> bool:
        """
        Whether part of the connection preamble is still outstanding.
        """
        return bool(self._preamble_len)

    def add_data(self, data: Buffer) -> None:
        """
        Add more data to the frame buffer.

        :param data: A bytestring containing the byte buffer.
        """
        data_view = memoryview(data)
        incoming_length = len(data_view)

        if (
            self.max_buffer_size is not None
            and self.buffered_data_length + incoming_length > self.max_buffer_size
        ):
            msg = (
                f"Inbound buffer would contain {self.buffered_data_length + incoming_length} "
                f"bytes, exceeding the limit of {self.max_buffer_size}"
            )
            raise InboundBufferTooLargeError(msg)

        if self._preamble_len:
            data_len = len(data_view)
            of_which_preamble = min(self._preamble_len, data_len)

            if self._preamble[:of_which_preamble] != data_view[:of_which_preamble]:
                msg = "Invalid HTTP/2 preamble."
                raise ProtocolError(msg)

            data_view = data_view[of_which_preamble:]
            self._preamble_len -= of_which_preamble
            self._preamble = self._preamble[of_which_preamble:]

        self._data += data_view

    def _parse_next_frame(self) -> Frame | None:
        """
        Parse and consume the next complete wire frame.

        A frame that starts or continues a header block is counted, even when
        it is not yet ready to be yielded to the connection.
        """
        if not self.has_complete_frame:
            return None

        try:
            f, length = Frame.parse_frame_header(memoryview(self._data[:9]))
        except (InvalidDataError, InvalidFrameError) as err:  # pragma: no cover
            msg = f"Received frame with invalid header: {err!s}"
            raise ProtocolError(msg) from err

        # Confirm the frame has an appropriate length.
        self._validate_frame_length(length)

        # Try to parse the frame body
        try:
            f.parse_body(memoryview(self._data[9:9+length]))
        except InvalidDataError as err:
            msg = "Received frame with non-compliant data"
            raise ProtocolError(msg) from err
        except InvalidFrameError as err:
            msg = "Frame data missing or invalid"
            raise FrameDataMissingError(msg) from err

        # At this point, as we know we'll use or discard the entire frame, we
        # can update the data.
        del self._data[:9+length]

        return self._update_header_buffer(f)

    def next_frame(self) -> Frame | None:
        """
        Return the next frame that is ready for the connection, or ``None``.
        """
        frame, _ = self.parse_frames()
        return frame

    def parse_frames(self, frame_budget: int | None = None) -> tuple[Frame | None, int]:
        """
        Parse up to ``frame_budget`` wire frames and return the next logical
        frame plus the number of wire frames consumed.
        """
        frames_parsed = 0

        while frame_budget is None or frames_parsed < frame_budget:
            if not self.has_complete_frame:
                break

            frame = self._parse_next_frame()
            frames_parsed += 1
            if frame is not None:
                return frame, frames_parsed

        return None, frames_parsed

    def _validate_frame_length(self, length: int) -> None:
        """
        Confirm that the frame is an appropriate length.
        """
        if length > self.max_frame_size:
            msg = f"Received overlong frame: length {length}, max {self.max_frame_size}"
            raise FrameTooLargeError(msg)

    def _update_header_buffer(self, f: Frame | None) -> Frame | None:
        """
        Updates the internal header buffer. Returns a frame that should replace
        the current one. May throw exceptions if this frame is invalid.
        """
        # Check if we're in the middle of a headers block. If we are, this
        # frame *must* be a CONTINUATION frame with the same stream ID as the
        # leading HEADERS or PUSH_PROMISE frame. Anything else is a
        # ProtocolError. If the frame *is* valid, append it to the header
        # buffer.
        if self._headers_buffer:
            stream_id = self._headers_buffer[0].stream_id
            valid_frame = (
                f is not None and
                isinstance(f, ContinuationFrame) and
                f.stream_id == stream_id
            )
            if not valid_frame:
                msg = "Invalid frame during header block."
                raise ProtocolError(msg)
            assert isinstance(f, ContinuationFrame)

            # Append the frame to the buffer.
            self._headers_buffer.append(f)
            if len(self._headers_buffer) > CONTINUATION_BACKLOG:
                msg = "Too many continuation frames received."
                raise ProtocolError(msg)

            # If this is the end of the header block, then we want to build a
            # mutant HEADERS frame that's massive. Use the original one we got,
            # then set END_HEADERS and set its data appropriately. If it's not
            # the end of the block, lose the current frame: we can't yield it.
            if "END_HEADERS" in f.flags:
                f = self._headers_buffer[0]
                f.flags.add("END_HEADERS")
                f.data = b"".join(x.data for x in self._headers_buffer)
                self._headers_buffer = []
            else:
                f = None
        elif (isinstance(f, (HeadersFrame, PushPromiseFrame)) and
                "END_HEADERS" not in f.flags):
            # This is the start of a headers block! Save the frame off and then
            # act like we didn't receive one.
            self._headers_buffer.append(f)
            f = None

        return f

    # The methods below support the iterator protocol.
    def __iter__(self) -> FrameBuffer:
        return self

    def __next__(self) -> Frame:
        frame = self.next_frame()
        if frame is None:
            raise StopIteration
        return frame
