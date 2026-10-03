"""
Tests for the extension frame handler interface (h2.extensions).

These tests cover registering per-connection handlers for custom HTTP/2
extension frame types, the core validation that happens before a handler
is invoked, and the error-handling contract for handler failures.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import ExtensionFrame, Frame, GoAwayFrame

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.extensions

# A frame type in the unassigned range: safe for extension use.
EXT_TYPE = 0xF1

REQUEST_HEADERS = [
    (":method", "GET"),
    (":path", "/"),
    (":authority", "example.com"),
    (":scheme", "https"),
]


class RecordingHandler(h2.extensions.ExtensionFrameHandler):
    """
    A minimal extension handler that records what it was called with and
    emits a single event per frame.
    """

    stream_association = h2.extensions.StreamAssociation.EITHER
    max_payload_size = 64

    def __init__(self, emit_events=False):
        self.received = []
        self.emit_events = emit_events

    def handle_frame(self, connection, stream_id, flags, payload):
        assert isinstance(payload, bytes)
        self.received.append((stream_id, flags, payload))
        if self.emit_events:
            return [h2.events.SettingsAcknowledged()]
        return None


def build_extension_frame(frame_type=EXT_TYPE, stream_id=0, flags=0, payload=b""):
    """
    Build the wire representation of an extension frame.
    """
    f = ExtensionFrame(frame_type, stream_id, flag_byte=flags, body=payload)
    f.body_len = len(payload)
    return f.serialize()


def make_connection(client_side):
    config = h2.config.H2Configuration(client_side=client_side)
    return h2.connection.H2Connection(config=config)


def connected_pair():
    """
    Return a (client, server) pair of connections that have exchanged
    their connection preambles.
    """
    client = make_connection(client_side=True)
    server = make_connection(client_side=False)
    client.initiate_connection()
    server.initiate_connection()
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())
    # Flush the settings ACKs in both directions so that later exchanges
    # only carry the frames under test.
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())
    server.receive_data(client.data_to_send())
    return client, server


class TestExtensionExchange:
    """
    A simple extension spoken between two peers.
    """

    def test_extension_round_trip_between_peers(self):
        client, server = connected_pair()
        client_handler = RecordingHandler()
        server_handler = RecordingHandler()
        client.register_extension_handler(EXT_TYPE, client_handler)
        server.register_extension_handler(EXT_TYPE, server_handler)

        # Client to server.
        client.send_extension_frame(EXT_TYPE, stream_id=1, flags=0x1, payload=b"hello")
        events = server.receive_data(client.data_to_send())
        assert server_handler.received == [(1, 0x1, b"hello")]
        assert events == []

        # Server to client.
        server.send_extension_frame(EXT_TYPE, stream_id=1, flags=0x2, payload=b"world")
        events = client.receive_data(server.data_to_send())
        assert client_handler.received == [(1, 0x2, b"world")]
        assert events == []

    def test_handler_events_are_returned_to_application(self):
        client, server = connected_pair()
        server.register_extension_handler(EXT_TYPE, RecordingHandler(emit_events=True))
        client.register_extension_handler(EXT_TYPE, RecordingHandler())

        client.send_extension_frame(EXT_TYPE, payload=b"x")
        events = server.receive_data(client.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)

    def test_registration_is_per_connection(self):
        client, server = connected_pair()
        server.register_extension_handler(EXT_TYPE, RecordingHandler())

        # A second server connection without a registration ignores the
        # frame, per RFC 9113 § 5.5, and fires UnknownFrameReceived.
        other_client = make_connection(client_side=True)
        other_server = make_connection(client_side=False)
        other_client.initiate_connection()
        other_server.initiate_connection()
        other_server.receive_data(other_client.data_to_send())

        frame_data = build_extension_frame(payload=b"data")
        events = other_server.receive_data(frame_data)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.UnknownFrameReceived)
        assert events[0].frame.type == EXT_TYPE


class TestRegistrationValidation:
    """
    The registration interface enforces its own contract.
    """

    @pytest.mark.parametrize("frame_type", range(0x0, 0xB))
    def test_cannot_override_standard_frame_types(self, frame_type):
        c = make_connection(client_side=True)
        with pytest.raises(ValueError, match="reserved frame type"):
            c.register_extension_handler(frame_type, RecordingHandler())

    @pytest.mark.parametrize("frame_type", [-1, 0x100, "5", None])
    def test_invalid_frame_type_rejected(self, frame_type):
        c = make_connection(client_side=True)
        with pytest.raises(ValueError, match="Invalid extension frame type"):
            c.register_extension_handler(frame_type, RecordingHandler())

    def test_duplicate_registration_rejected(self):
        c = make_connection(client_side=True)
        c.register_extension_handler(EXT_TYPE, RecordingHandler())
        with pytest.raises(ValueError, match="already registered"):
            c.register_extension_handler(EXT_TYPE, RecordingHandler())

    def test_handler_must_define_handle_frame(self):
        c = make_connection(client_side=True)

        class NotAHandler:
            stream_association = h2.extensions.StreamAssociation.EITHER
            max_payload_size = 16

        with pytest.raises(TypeError, match="handle_frame"):
            c.register_extension_handler(EXT_TYPE, NotAHandler())

    def test_handler_must_declare_stream_association(self):
        c = make_connection(client_side=True)

        class BadAssociation(RecordingHandler):
            stream_association = "stream"

        with pytest.raises(TypeError, match="stream_association"):
            c.register_extension_handler(EXT_TYPE, BadAssociation())

    @pytest.mark.parametrize("bad_size", [-1, "64", None, True])
    def test_handler_must_declare_valid_payload_limit(self, bad_size):
        c = make_connection(client_side=True)

        class BadLimit(RecordingHandler):
            max_payload_size = bad_size

        with pytest.raises(TypeError, match="max_payload_size"):
            c.register_extension_handler(EXT_TYPE, BadLimit())


class TestCoreValidation:
    """
    The core validates frames before the handler is invoked.
    """

    def test_extension_frame_rejected_inside_header_block(self, frame_factory):
        """
        An extension frame interleaved into an unfinished header block is
        rejected by the core protocol rules: the handler never runs.
        """
        client, server = connected_pair()
        handler = RecordingHandler()
        server.register_extension_handler(EXT_TYPE, handler)

        headers_frame = frame_factory.build_headers_frame(REQUEST_HEADERS)
        headers_frame.flags.discard("END_HEADERS")
        data = headers_frame.serialize() + build_extension_frame(
            stream_id=1, payload=b"sneaky",
        )

        with pytest.raises(h2.exceptions.ProtocolError, match="Invalid frame during header block"):
            server.receive_data(data)

        assert handler.received == []

    def test_payload_limit_enforced_on_receive(self):
        client, server = connected_pair()

        class TinyHandler(RecordingHandler):
            max_payload_size = 4

        handler = TinyHandler()
        server.register_extension_handler(EXT_TYPE, handler)

        data = build_extension_frame(payload=b"this is too long")
        with pytest.raises(h2.exceptions.FrameTooLargeError):
            server.receive_data(data)

        assert handler.received == []

    def test_payload_limit_enforced_on_send(self):
        client, _ = connected_pair()

        class TinyHandler(RecordingHandler):
            max_payload_size = 4

        client.register_extension_handler(EXT_TYPE, TinyHandler())
        with pytest.raises(h2.exceptions.FrameTooLargeError):
            client.send_extension_frame(EXT_TYPE, payload=b"this is too long")

    @pytest.mark.parametrize("association,bad_stream_id", [
        (h2.extensions.StreamAssociation.CONNECTION, 1),
        (h2.extensions.StreamAssociation.STREAM, 0),
    ])
    def test_stream_association_enforced_on_receive(self, association, bad_stream_id):
        client, server = connected_pair()

        class StrictHandler(RecordingHandler):
            stream_association = association

        handler = StrictHandler()
        server.register_extension_handler(EXT_TYPE, handler)

        data = build_extension_frame(stream_id=bad_stream_id, payload=b"x")
        with pytest.raises(h2.exceptions.ProtocolError, match="extension frame"):
            server.receive_data(data)

        assert handler.received == []

    @pytest.mark.parametrize("association,bad_stream_id", [
        (h2.extensions.StreamAssociation.CONNECTION, 1),
        (h2.extensions.StreamAssociation.STREAM, 0),
    ])
    def test_stream_association_enforced_on_send(self, association, bad_stream_id):
        client, _ = connected_pair()

        class StrictHandler(RecordingHandler):
            stream_association = association

        client.register_extension_handler(EXT_TYPE, StrictHandler())
        with pytest.raises(h2.exceptions.ProtocolError, match="extension frame"):
            client.send_extension_frame(EXT_TYPE, stream_id=bad_stream_id, payload=b"x")

    def test_send_requires_registration(self):
        client, _ = connected_pair()
        with pytest.raises(ValueError, match="No extension handler registered"):
            client.send_extension_frame(EXT_TYPE, payload=b"x")

    def test_send_on_closed_connection_rejected(self):
        client, server = connected_pair()
        client.register_extension_handler(EXT_TYPE, RecordingHandler())
        client.close_connection()
        server.receive_data(client.data_to_send())

        with pytest.raises(h2.exceptions.ProtocolError, match="closed"):
            client.send_extension_frame(EXT_TYPE, payload=b"x")

    @pytest.mark.parametrize("flags", [-1, 0x100, "1"])
    def test_send_with_invalid_flags_rejected(self, flags):
        client, _ = connected_pair()
        client.register_extension_handler(EXT_TYPE, RecordingHandler())
        with pytest.raises(ValueError, match="Invalid flag byte"):
            client.send_extension_frame(EXT_TYPE, flags=flags, payload=b"x")

    @pytest.mark.parametrize("stream_id", [-1, 2**31, "1"])
    def test_send_with_invalid_stream_id_rejected(self, stream_id):
        client, _ = connected_pair()
        client.register_extension_handler(EXT_TYPE, RecordingHandler())
        with pytest.raises(ValueError, match="Invalid stream ID"):
            client.send_extension_frame(EXT_TYPE, stream_id=stream_id, payload=b"x")

    def test_send_payload_larger_than_outbound_frame_size_rejected(self):
        client, _ = connected_pair()

        class GreedyHandler(RecordingHandler):
            max_payload_size = 2**20

        client.register_extension_handler(EXT_TYPE, GreedyHandler())
        with pytest.raises(h2.exceptions.FrameTooLargeError):
            client.send_extension_frame(EXT_TYPE, payload=b"x" * (2**14 + 1))

    def test_receive_on_closed_connection_rejected(self):
        client, server = connected_pair()
        handler = RecordingHandler()
        server.register_extension_handler(EXT_TYPE, handler)

        client.close_connection()
        server.receive_data(client.data_to_send())

        data = build_extension_frame(payload=b"x")
        with pytest.raises(h2.exceptions.ProtocolError, match="closed connection"):
            server.receive_data(data)

        assert handler.received == []


class TestHandlerFailureModes:
    """
    Handler exceptions map to protocol errors or application errors.
    """

    def test_protocol_error_terminates_connection(self):
        client, server = connected_pair()

        class PickyHandler(RecordingHandler):
            def handle_frame(self, connection, stream_id, flags, payload):
                msg = "extension protocol violated"
                raise h2.exceptions.ExtensionProtocolError(msg)

        server.register_extension_handler(EXT_TYPE, PickyHandler())
        client.register_extension_handler(EXT_TYPE, RecordingHandler())

        client.send_extension_frame(EXT_TYPE, payload=b"bad")
        with pytest.raises(h2.exceptions.ExtensionProtocolError):
            server.receive_data(client.data_to_send())

        # The core emitted a GOAWAY frame carrying the error.
        outbound = server.data_to_send()
        frame, length = Frame.parse_frame_header(memoryview(outbound[:9]))
        frame.parse_body(memoryview(outbound[9:9 + length]))
        assert isinstance(frame, GoAwayFrame)
        assert frame.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_application_error_leaves_connection_intact(self):
        client, server = connected_pair()

        class FlakyHandler(RecordingHandler):
            fail = True

            def handle_frame(self, connection, stream_id, flags, payload):
                if self.fail:
                    self.fail = False
                    msg = "application bug"
                    raise RuntimeError(msg)
                return super().handle_frame(connection, stream_id, flags, payload)

        handler = FlakyHandler()
        server.register_extension_handler(EXT_TYPE, handler)
        client.register_extension_handler(EXT_TYPE, RecordingHandler())

        client.send_extension_frame(EXT_TYPE, payload=b"boom")
        with pytest.raises(h2.exceptions.ExtensionApplicationError) as exc_info:
            server.receive_data(client.data_to_send())

        assert isinstance(exc_info.value.__cause__, RuntimeError)

        # No GOAWAY was emitted, and the connection still works: a
        # subsequent frame is dispatched to the handler normally.
        assert server.data_to_send() == b""
        client.send_extension_frame(EXT_TYPE, payload=b"fine")
        server.receive_data(client.data_to_send())
        assert handler.received == [(0, 0, b"fine")]
