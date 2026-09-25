"""
Tests for RFC 9218 (Extensible Prioritization Scheme for HTTP) support:
the SETTINGS_NO_RFC7540_PRIORITIES setting, the PRIORITY_UPDATE frame, and
the PriorityUpdateReceived event.

These tests exchange real frame bytes between two H2Connection objects, one
acting as the client and one as the server.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import SettingsFrame

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings
from h2._rfc9218 import PriorityUpdateFrame


class TestPriorityUpdate:
    """
    Tests for the RFC 9218 PRIORITY_UPDATE frame and its associated setting.
    """

    server_config = h2.config.H2Configuration(client_side=False)

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]

    example_response_headers = [
        (":status", "200"),
        ("server", "pytest-h2"),
    ]

    def _connected_pair(self):
        """
        Return a (client, server) pair of H2Connections that have completed
        the HTTP/2 handshake by exchanging real frame bytes.
        """
        client = h2.connection.H2Connection()
        server = h2.connection.H2Connection(config=self.server_config)

        client.initiate_connection()
        server.initiate_connection()

        server.receive_data(client.data_to_send())
        client.receive_data(server.data_to_send())
        server.receive_data(client.data_to_send())

        client.clear_outbound_data_buffer()
        server.clear_outbound_data_buffer()

        return client, server

    def _exchange(self, sender, receiver):
        """
        Move all pending bytes from one connection to the other and return
        the events emitted on the receiving side.
        """
        return receiver.receive_data(sender.data_to_send())

    def test_no_rfc7540_priorities_setting_is_negotiated(self) -> None:
        """
        The NO_RFC7540_PRIORITIES setting defaults to 0 and can be updated to
        1, which is visible to the remote peer.
        """
        client, server = self._connected_pair()

        setting = h2.settings.SettingCodes.NO_RFC7540_PRIORITIES
        assert client.local_settings.no_rfc7540_priorities == 0
        assert server.remote_settings.no_rfc7540_priorities == 0

        client.update_settings({setting: 1})
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.RemoteSettingsChanged)
        changed = event.changed_settings[setting]
        assert changed.original_value is None
        assert changed.new_value == 1
        assert server.remote_settings.no_rfc7540_priorities == 1

        # The server acknowledges the settings, and the acknowledgement
        # applies the new value to the client's local settings.
        events = self._exchange(server, client)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)
        assert client.local_settings.no_rfc7540_priorities == 1

    @pytest.mark.parametrize("value", [2, 256, 4294967295])
    def test_no_rfc7540_priorities_rejects_invalid_values(self, value) -> None:
        """
        A SETTINGS frame carrying an out-of-range NO_RFC7540_PRIORITIES value
        is a connection error of type PROTOCOL_ERROR (RFC 9218 section 4.1).
        """
        client, server = self._connected_pair()

        f = SettingsFrame(0)
        f.settings = {h2.settings.SettingCodes.NO_RFC7540_PRIORITIES: value}

        with pytest.raises(h2.exceptions.ProtocolError) as exc_info:
            server.receive_data(f.serialize())
        assert exc_info.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_client_priority_update_is_received_by_server(self) -> None:
        """
        A client-sent PRIORITY_UPDATE frame on stream 0 produces a
        PriorityUpdateReceived event on the server carrying the urgency and
        incremental information.
        """
        client, server = self._connected_pair()

        client.priority_update(
            0, prioritized_element_id=1, urgency=0, incremental=True,
        )
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.stream_id == 0
        assert event.prioritized_element_id == 1
        assert event.priority_field_value == b"u=0, i"
        assert event.urgency == 0
        assert event.incremental is True

    def test_server_priority_update_is_received_by_client(self) -> None:
        """
        A server-sent PRIORITY_UPDATE frame on a push stream produces a
        PriorityUpdateReceived event on the client.
        """
        client, server = self._connected_pair()

        server.priority_update(2, urgency=7)
        events = self._exchange(server, client)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.stream_id == 2
        assert event.prioritized_element_id == 2
        assert event.priority_field_value == b"u=7"
        assert event.urgency == 7
        assert event.incremental is False

    def test_priority_update_defaults_are_omitted(self) -> None:
        """
        Default urgency and incremental values serialize to an empty field
        value, and are parsed back to the defaults.
        """
        client, server = self._connected_pair()

        client.priority_update(0, prioritized_element_id=1)
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert event.priority_field_value == b""
        assert event.urgency == 3
        assert event.incremental is False

    @pytest.mark.parametrize(
        ("field_value", "expected_urgency", "expected_incremental"),
        [
            # Unknown dictionary members are ignored.
            (b"u=1, x-unknown=5, i", 1, True),
            # Parameters on known and unknown members are ignored.
            (b"u=5;foo=bar, i;baz=?1", 5, True),
            # Out-of-range urgencies fall back to the default.
            (b"u=8", 3, False),
            (b"u=-1", 3, False),
            # Wrong-typed values fall back to the defaults.
            (b"u=1.5, i=?0", 3, False),
            (b'u="high", i="yes"', 3, False),
            # Explicit false incremental flag.
            (b"u=2, i=?0", 2, False),
            # Garbage that is not a dictionary falls back to the defaults.
            (b"this is not a dictionary =", 3, False),
        ],
    )
    def test_priority_update_ignores_unknown_and_invalid_parameters(
        self, field_value, expected_urgency, expected_incremental,
    ) -> None:
        """
        Unknown members, parameters, and invalid values in the Priority field
        value are ignored, per RFC 9218 section 4.
        """
        client, server = self._connected_pair()

        client.priority_update(
            0, prioritized_element_id=1, priority_field_value=field_value,
        )
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert event.urgency == expected_urgency
        assert event.incremental is expected_incremental

    def test_priority_update_frame_too_short(self) -> None:
        """
        A PRIORITY_UPDATE frame shorter than 4 octets is a connection error
        of type FRAME_SIZE_ERROR (RFC 9218 section 6).
        """
        client, server = self._connected_pair()

        # A PRIORITY_UPDATE frame (type 0x10) on stream 0 with a 2-byte body.
        frame_bytes = b"\x00\x00\x02\x10\x00\x00\x00\x00\x00" + b"\x00\x01"

        with pytest.raises(h2.exceptions.FrameDataMissingError) as exc_info:
            server.receive_data(frame_bytes)
        assert exc_info.value.error_code == h2.errors.ErrorCodes.FRAME_SIZE_ERROR

    def test_server_rejects_priority_update_on_stream(self) -> None:
        """
        A server that receives a PRIORITY_UPDATE frame on a stream other than
        stream 0 treats it as a connection error of type PROTOCOL_ERROR.
        """
        client, server = self._connected_pair()

        f = PriorityUpdateFrame(
            1, prioritized_element_id=1, priority_field_value=b"u=1",
        )

        with pytest.raises(h2.exceptions.ProtocolError) as exc_info:
            server.receive_data(f.serialize())
        assert exc_info.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_client_rejects_priority_update_on_stream_zero(self) -> None:
        """
        A client that receives a PRIORITY_UPDATE frame on stream 0 treats it
        as a connection error of type PROTOCOL_ERROR.
        """
        client, server = self._connected_pair()

        f = PriorityUpdateFrame(
            0, prioritized_element_id=2, priority_field_value=b"u=1",
        )

        with pytest.raises(h2.exceptions.ProtocolError) as exc_info:
            client.receive_data(f.serialize())
        assert exc_info.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_client_rejects_priority_update_on_non_push_stream(self) -> None:
        """
        A client that receives a PRIORITY_UPDATE frame on a stream that is
        not a push stream treats it as a stream error of type PROTOCOL_ERROR.
        """
        client, server = self._connected_pair()

        client.send_headers(1, self.example_request_headers)
        server.receive_data(client.data_to_send())

        f = PriorityUpdateFrame(
            1, prioritized_element_id=1, priority_field_value=b"u=1",
        )
        events = client.receive_data(f.serialize())

        # No event is emitted, but a RST_STREAM is queued for the stream.
        assert events == []
        data = client.data_to_send()
        # 9-byte header, type 0x3 (RST_STREAM), stream 1, error PROTOCOL_ERROR.
        assert data == b"\x00\x00\x04\x03\x00\x00\x00\x00\x01" + b"\x00\x00\x00\x01"

    def test_priority_update_for_closed_stream(self) -> None:
        """
        A PRIORITY_UPDATE frame referencing a closed stream is still
        surfaced to the application, and does not cause any stream state to
        be retained.
        """
        client, server = self._connected_pair()

        # Open and close stream 1.
        client.send_headers(1, self.example_request_headers, end_stream=True)
        server.receive_data(client.data_to_send())
        server.send_headers(1, self.example_response_headers, end_stream=True)
        client.receive_data(server.data_to_send())
        # Force the connection to garbage-collect the closed stream.
        server.open_inbound_streams
        assert 1 not in server.streams

        client.priority_update(0, prioritized_element_id=1, urgency=6)
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.prioritized_element_id == 1
        assert event.urgency == 6

        # No stream state is retained for the closed stream.
        assert 1 not in server.streams

    def test_priority_update_for_idle_stream_retains_no_state(self) -> None:
        """
        PRIORITY_UPDATE frames for streams that have never existed do not
        cause unbounded state retention: the priority information is passed
        to the application and then forgotten.
        """
        client, server = self._connected_pair()

        for stream_id in range(1, 1000, 2):
            client.priority_update(
                0, prioritized_element_id=stream_id, urgency=5,
            )
            events = self._exchange(client, server)
            assert len(events) == 1
            assert events[0].prioritized_element_id == stream_id

        assert not server.streams

    def test_rfc7540_priority_interface_still_works(self) -> None:
        """
        The RFC 7540 priority tree interface is unaffected by the RFC 9218
        support.
        """
        client, server = self._connected_pair()

        client.prioritize(1, weight=200, depends_on=0, exclusive=False)
        events = self._exchange(client, server)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdated)
        assert event.stream_id == 1
        assert event.weight == 200

    def test_sending_priority_update_enforces_roles(self) -> None:
        """
        Clients may not send PRIORITY_UPDATE on a stream, and servers may not
        send it on stream 0.
        """
        client, server = self._connected_pair()

        with pytest.raises(h2.exceptions.ProtocolError):
            client.priority_update(1, prioritized_element_id=1)
        with pytest.raises(h2.exceptions.ProtocolError):
            server.priority_update(0, prioritized_element_id=2)
        with pytest.raises(h2.exceptions.ProtocolError):
            client.priority_update(0)

    def test_sending_priority_update_validates_urgency(self) -> None:
        """
        Urgency values outside 0-7 are rejected when sending.
        """
        client, _ = self._connected_pair()

        with pytest.raises(h2.exceptions.ProtocolError):
            client.priority_update(0, prioritized_element_id=1, urgency=8)
