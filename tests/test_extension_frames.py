"""
Tests for privately negotiated HTTP/2 extension frames.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from hyperframe.frame import (
    ContinuationFrame,
    ExtensionFrame,
    Frame,
    GoAwayFrame,
    HeadersFrame,
    RstStreamFrame,
)
from hpack.hpack import Encoder

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
from h2.extensions import (
    ExtensionFrameHandler,
    ExtensionFrameScope,
    ReceivedExtensionFrame,
)


@dataclass(frozen=True)
class ExtensionMessageReceived(h2.events.Event):
    frame_type: int
    stream_id: int
    flags: int
    payload: bytes


class RecordingHandler(ExtensionFrameHandler):
    def __init__(
        self,
        frame_type: int = 33,
        scope: ExtensionFrameScope = ExtensionFrameScope.CONNECTION,
        max_payload_size: int = 256,
        failure: Exception | None = None,
    ) -> None:
        self._frame_type = frame_type
        self._scope = scope
        self._max_payload_size = max_payload_size
        self.failure = failure
        self.frames: list[ReceivedExtensionFrame] = []

    @property
    def frame_type(self) -> int:
        return self._frame_type

    @property
    def scope(self) -> ExtensionFrameScope:
        return self._scope

    @property
    def max_payload_size(self) -> int:
        return self._max_payload_size

    def receive_frame(
        self,
        frame: ReceivedExtensionFrame,
    ) -> list[h2.events.Event]:
        self.frames.append(frame)
        if self.failure is not None:
            raise self.failure
        return [
            ExtensionMessageReceived(
                frame_type=frame.frame_type,
                stream_id=frame.stream_id,
                flags=frame.flags,
                payload=frame.payload,
            ),
        ]


class TestExtensionFrames:
    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]

    def make_connection(self, client_side: bool) -> h2.connection.H2Connection:
        config = h2.config.H2Configuration(client_side=client_side)
        return h2.connection.H2Connection(config=config)

    def test_extension_frame_is_exchanged_between_both_peers(self) -> None:
        client = self.make_connection(client_side=True)
        server = self.make_connection(client_side=False)
        client_handler = RecordingHandler()
        server_handler = RecordingHandler()
        client.register_extension_frame_handler(client_handler)
        server.register_extension_frame_handler(server_handler)
        client.initiate_connection()
        server.initiate_connection()

        client.send_extension_frame(
            frame_type=33,
            payload=b"ping",
            flags=0x42,
        )
        server_events = server.receive_data(client.data_to_send())
        server.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

        server.send_extension_frame(frame_type=33, payload=b"pong")
        client_events = client.receive_data(server.data_to_send())

        assert len(server_handler.frames) == 1
        assert server_handler.frames[0] == ReceivedExtensionFrame(
            frame_type=33,
            stream_id=0,
            flags=0x42,
            payload=b"ping",
        )
        assert isinstance(server_events[-1], ExtensionMessageReceived)
        assert server_events[-1].payload == b"ping"
        assert len(client_handler.frames) == 1
        assert client_handler.frames[0].payload == b"pong"
        assert isinstance(client_events[-1], ExtensionMessageReceived)

    @pytest.mark.parametrize("frame_type", range(11))
    def test_standard_frame_types_cannot_be_registered(
        self,
        frame_type: int,
    ) -> None:
        connection = self.make_connection(client_side=True)
        handler = RecordingHandler(frame_type=frame_type)

        with pytest.raises(ValueError, match="reserved"):
            connection.register_extension_frame_handler(handler)

    def test_duplicate_registration_is_rejected(self) -> None:
        connection = self.make_connection(client_side=True)
        connection.register_extension_frame_handler(RecordingHandler())

        with pytest.raises(ValueError, match="already registered"):
            connection.register_extension_frame_handler(RecordingHandler())

    def test_invalid_registration_metadata_is_rejected(self) -> None:
        connection = self.make_connection(client_side=True)

        with pytest.raises(TypeError):
            connection.register_extension_frame_handler(object())  # type: ignore[arg-type]

        handler = RecordingHandler(max_payload_size=-1)
        with pytest.raises(ValueError, match="payload size"):
            connection.register_extension_frame_handler(handler)

    def test_registration_is_scoped_to_one_connection(self) -> None:
        registered = self.make_connection(client_side=True)
        unregistered = self.make_connection(client_side=False)
        registered.register_extension_frame_handler(RecordingHandler())
        registered.initiate_connection()
        unregistered.initiate_connection()

        registered.send_extension_frame(frame_type=33, payload=b"x")
        events = unregistered.receive_data(registered.data_to_send())

        assert isinstance(events[-1], h2.events.UnknownFrameReceived)

    def test_handler_payload_is_immutable(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler()
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

        frame = self.extension_frame(payload=b"abc")
        connection.receive_data(frame.serialize())

        delivered = handler.frames[0]
        with pytest.raises(AttributeError):
            delivered.payload = b"changed"  # type: ignore[misc]

    def test_payload_limit_is_enforced_before_handler(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler(max_payload_size=2)
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.clear_outbound_data_buffer()
        frame = self.extension_frame(payload=b"abc")

        with pytest.raises(h2.exceptions.FrameTooLargeError):
            connection.receive_data(frame.serialize())

        assert handler.frames == []
        error_frame = self.parse_single_frame(connection.data_to_send())
        assert isinstance(error_frame, GoAwayFrame)
        assert error_frame.error_code == h2.errors.ErrorCodes.FRAME_SIZE_ERROR

    def test_negotiated_frame_size_is_checked_before_handler(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler(max_payload_size=2**24)
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.local_settings.max_frame_size = 16384
        connection.max_inbound_frame_size = 16384
        connection.clear_outbound_data_buffer()
        frame = self.extension_frame(payload=b"x" * 16385)

        with pytest.raises(h2.exceptions.FrameTooLargeError):
            connection.receive_data(frame.serialize())

        assert handler.frames == []

    def test_connection_scope_rejects_stream_identifier(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler()
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.clear_outbound_data_buffer()
        frame = self.extension_frame(stream_id=1)

        with pytest.raises(h2.exceptions.ProtocolError, match="stream ID 0"):
            connection.receive_data(frame.serialize())

        assert handler.frames == []
        error_frame = self.parse_single_frame(connection.data_to_send())
        assert isinstance(error_frame, GoAwayFrame)

    def test_stream_scope_requires_existing_stream(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler(scope=ExtensionFrameScope.STREAM)
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.clear_outbound_data_buffer()
        frame = self.extension_frame(stream_id=5)

        with pytest.raises(h2.exceptions.NoSuchStreamError):
            connection.receive_data(frame.serialize())

        assert handler.frames == []

    def test_handler_protocol_error_terminates_connection(self) -> None:
        connection = self.make_connection(client_side=False)
        failure = h2.exceptions.ExtensionProtocolError("extension rejected")
        handler = RecordingHandler(failure=failure)
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ExtensionProtocolError, match="rejected"):
            connection.receive_data(
                self.extension_frame().serialize(),
            )

        error_frame = self.parse_single_frame(connection.data_to_send())
        assert isinstance(error_frame, GoAwayFrame)
        assert error_frame.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_stream_protocol_error_sends_rst_stream(self) -> None:
        connection = self.make_connection(client_side=False)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        request_headers = HeadersFrame(stream_id=1)
        request_headers.data = Encoder().encode(self.example_request_headers)
        request_headers.flags.add("END_HEADERS")
        request_headers.flags.add("END_STREAM")
        connection.receive_data(request_headers.serialize())
        connection.clear_outbound_data_buffer()

        failure = h2.exceptions.ExtensionProtocolError(
            "bad stream frame",
            h2.errors.ErrorCodes.PROTOCOL_ERROR,
            stream_error=True,
        )
        handler = RecordingHandler(
            scope=ExtensionFrameScope.STREAM,
            failure=failure,
        )
        connection.register_extension_frame_handler(handler)

        with pytest.raises(h2.exceptions.ExtensionProtocolError):
            connection.receive_data(
                self.extension_frame(stream_id=1).serialize(),
            )

        error_frame = self.parse_single_frame(connection.data_to_send())
        assert isinstance(error_frame, RstStreamFrame)
        assert error_frame.stream_id == 1
        assert error_frame.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_handler_runtime_error_becomes_application_error(self) -> None:
        client = self.make_connection(client_side=True)
        client.initiate_connection()
        client.send_headers(stream_id=1, headers=self.example_request_headers)
        client.clear_outbound_data_buffer()
        state = client.state_machine.state
        handler = RecordingHandler(failure=RuntimeError("boom"))
        client.register_extension_frame_handler(handler)

        with pytest.raises(h2.exceptions.ExtensionApplicationError, match="boom"):
            client.receive_data(
                self.extension_frame().serialize(),
            )

        assert client.data_to_send() == b""
        assert client.state_machine.state is state
        client.ping(b"12345678")
        assert client.data_to_send()

    def test_extension_frame_is_rejected_after_connection_closes(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler()
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.close_connection()
        connection.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError, match="closed connection"):
            connection.receive_data(self.extension_frame().serialize())

        assert handler.frames == []
        error_frame = self.parse_single_frame(connection.data_to_send())
        assert isinstance(error_frame, GoAwayFrame)

    def test_extension_frame_cannot_be_sent_after_connection_closes(self) -> None:
        connection = self.make_connection(client_side=True)
        connection.initiate_connection()
        connection.close_connection()

        with pytest.raises(h2.exceptions.ProtocolError, match="closed connection"):
            connection.send_extension_frame(frame_type=33)

    def test_handler_can_be_unregistered_for_connection(self) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler()
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

        connection.unregister_extension_frame_handler(33)
        events = connection.receive_data(self.extension_frame().serialize())

        assert handler.frames == []
        assert isinstance(events[0], h2.events.UnknownFrameReceived)

    def test_frame_in_unfinished_header_block_never_reaches_handler(
        self,
    ) -> None:
        connection = self.make_connection(client_side=False)
        handler = RecordingHandler()
        connection.register_extension_frame_handler(handler)
        connection.initiate_connection()
        connection.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        connection.clear_outbound_data_buffer()

        headers = HeadersFrame(stream_id=1)
        encoded_headers = b"".join(
            [
                b"\x82",
                b"\x86",
                b"\x84",
                b"\x41",
                b"\x0bexample.com",
                b"\x88",
            ],
        )
        headers.data = encoded_headers[:2]
        continuation = ContinuationFrame(stream_id=1)
        continuation.data = encoded_headers[2:]
        continuation.flags.add("END_HEADERS")
        extension = self.extension_frame()
        data = (
            headers.serialize()
            + extension.serialize()
            + continuation.serialize()
        )

        with pytest.raises(h2.exceptions.ProtocolError, match="header block"):
            connection.receive_data(data)

        assert handler.frames == []

    @staticmethod
    def extension_frame(
        frame_type: int = 33,
        stream_id: int = 0,
        payload: bytes = b"x",
        flags: int = 0,
    ) -> ExtensionFrame:
        frame = ExtensionFrame(
            type=frame_type,
            stream_id=stream_id,
            flag_byte=flags,
            body=payload,
        )
        frame.body_len = len(payload)
        return frame

    @staticmethod
    def parse_single_frame(data: bytes) -> Frame:
        frame, length = Frame.parse_frame_header(memoryview(data[:9]))
        frame.parse_body(memoryview(data[9:9 + length]))
        return frame
