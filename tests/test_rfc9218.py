"""
Tests for RFC 9218 (Extensible Prioritization Scheme for HTTP): the
NO_RFC7540_PRIORITIES setting, the PRIORITY_UPDATE frame, and the
PriorityUpdateReceived event.

These tests exchange real frame bytes between two H2Connection objects, one
acting as the client and one as the server.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import Frame, GoAwayFrame, SettingsFrame

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.priority
import h2.settings


class TestRFC9218:
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

    def _connection_pair(self):
        """
        Returns a connected (client, server) pair of H2Connection objects
        that have completed the HTTP/2 preamble and SETTINGS exchange.
        """
        client = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True),
        )
        server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False),
        )
        client.initiate_connection()
        server.initiate_connection()
        server.receive_data(client.data_to_send())
        client.receive_data(server.data_to_send())
        client.clear_outbound_data_buffer()
        server.clear_outbound_data_buffer()
        return client, server

    def _open_stream(self, client, server, stream_id=1):
        """
        Opens a stream on both peers by sending a request from the client.
        """
        client.send_headers(stream_id, self.example_request_headers)
        server.receive_data(client.data_to_send())

    def _received_goaway(self, connection):
        """
        Parses the GOAWAY frame the connection has queued for sending.
        """
        data = connection.data_to_send()
        frame, length = Frame.parse_frame_header(memoryview(data[:9]))
        assert isinstance(frame, GoAwayFrame)
        frame.parse_body(memoryview(data[9:9 + length]))
        return frame

    def test_settings_negotiation(self):
        """
        The NO_RFC7540_PRIORITIES setting can be exchanged and is surfaced
        via the RemoteSettingsChanged and SettingsAcknowledged events.
        """
        client, server = self._connection_pair()

        client.update_settings({
            h2.settings.SettingCodes.NO_RFC7540_PRIORITIES: 1,
        })
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.RemoteSettingsChanged)
        changed = event.changed_settings[
            h2.settings.SettingCodes.NO_RFC7540_PRIORITIES
        ]
        assert changed.original_value is None
        assert changed.new_value == 1
        assert server.remote_settings.no_rfc7540_priorities == 1

        # The acknowledgement flows back to the client.
        events = client.receive_data(server.data_to_send())
        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.SettingsAcknowledged)
        changed = event.changed_settings[
            h2.settings.SettingCodes.NO_RFC7540_PRIORITIES
        ]
        assert changed.original_value is None
        assert changed.new_value == 1
        assert client.local_settings.no_rfc7540_priorities == 1

    def test_invalid_setting_value_rejected(self):
        """
        A NO_RFC7540_PRIORITIES value other than 0 or 1 is a connection
        error of type PROTOCOL_ERROR.
        """
        client, server = self._connection_pair()

        frame = SettingsFrame(0)
        frame.settings = {h2.settings.SettingCodes.NO_RFC7540_PRIORITIES: 2}

        with pytest.raises(h2.exceptions.InvalidSettingsValueError) as exc:
            server.receive_data(frame.serialize())
        assert exc.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

        goaway = self._received_goaway(server)
        assert goaway.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_client_priority_update(self):
        """
        A client can reprioritize a stream: the server receives a
        PriorityUpdateReceived event carrying the urgency and incremental
        parameters.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        client.priority_update(1, urgency=0, incremental=True)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.stream_id == 0
        assert event.prioritized_element_id == 1
        assert event.urgency == 0
        assert event.incremental is True
        assert event.field_value == b"u=0, i"

    def test_priority_update_defaults(self):
        """
        Omitting urgency and incremental produces the RFC 9218 defaults.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        client.priority_update(1)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 3
        assert event.incremental is False
        assert event.field_value == b"u=3"

    @pytest.mark.parametrize("urgency", [-1, 8, 100])
    def test_invalid_urgency_rejected_on_send(self, urgency):
        """
        Urgency values outside 0-7 cannot be sent.
        """
        client, server = self._connection_pair()

        with pytest.raises(ValueError, match="Urgency"):
            client.priority_update(1, urgency=urgency)

        assert not client.data_to_send()

    def test_unknown_parameters_ignored(self):
        """
        Unknown parameters in the priority field value are ignored, while
        the known ones are still parsed.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        frame = h2.priority.PriorityUpdateFrame(0)
        frame.prioritized_element_id = 1
        frame.field_value = b"u=1, x-unknown=7, i=?1, another"
        events = server.receive_data(frame.serialize())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 1
        assert event.incremental is True
        assert event.field_value == b"u=1, x-unknown=7, i=?1, another"

    @pytest.mark.parametrize("field_value,expected_urgency,expected_incremental", [
        (b"u=9", 3, False),            # out-of-range urgency: default
        (b"u=-1", 3, False),           # negative urgency: default
        (b"u=abc", 3, False),          # malformed urgency: default
        (b"", 3, False),               # empty field value: defaults
        (b"\xff\xfe", 3, False),       # non-ASCII field value: defaults
        (b"i=?0", 3, False),           # explicit non-incremental
        (b"i", 3, True),               # bare incremental marker
        (b"u=7,i", 7, True),           # no whitespace
    ])
    def test_field_value_parsing_edge_cases(
        self, field_value, expected_urgency, expected_incremental,
    ):
        """
        Unusable field value components fall back to the RFC 9218 defaults.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        frame = h2.priority.PriorityUpdateFrame(0)
        frame.prioritized_element_id = 1
        frame.field_value = field_value
        events = server.receive_data(frame.serialize())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == expected_urgency
        assert event.incremental is expected_incremental

    def test_short_frame_is_frame_size_error(self):
        """
        A PRIORITY_UPDATE frame with a payload shorter than 4 bytes is a
        connection error of type FRAME_SIZE_ERROR.
        """
        client, server = self._connection_pair()

        # Build a raw PRIORITY_UPDATE frame with a 3-byte payload.
        frame = h2.priority.PriorityUpdateFrame(0)
        frame.field_value = b""
        data = bytearray(frame.serialize())
        data[0:3] = b"\x00\x00\x03"
        data = bytes(data[:9]) + b"\x00\x00\x00"

        with pytest.raises(h2.exceptions.ProtocolError) as exc:
            server.receive_data(data)
        assert exc.value.error_code == h2.errors.ErrorCodes.FRAME_SIZE_ERROR

        goaway = self._received_goaway(server)
        assert goaway.error_code == h2.errors.ErrorCodes.FRAME_SIZE_ERROR

    def test_server_rejects_priority_update_on_stream(self):
        """
        A client sending PRIORITY_UPDATE on a stream other than stream 0 is
        a connection error of type PROTOCOL_ERROR.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        frame = h2.priority.PriorityUpdateFrame(1)
        frame.prioritized_element_id = 1
        frame.field_value = b"u=3"

        with pytest.raises(h2.exceptions.ProtocolError) as exc:
            server.receive_data(frame.serialize())
        assert exc.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

        goaway = self._received_goaway(server)
        assert goaway.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_client_rejects_priority_update_on_stream_zero(self):
        """
        A server sending PRIORITY_UPDATE on stream 0 is a connection error
        of type PROTOCOL_ERROR.
        """
        client, server = self._connection_pair()

        frame = h2.priority.PriorityUpdateFrame(0)
        frame.prioritized_element_id = 2
        frame.field_value = b"u=3"

        with pytest.raises(h2.exceptions.ProtocolError) as exc:
            client.receive_data(frame.serialize())
        assert exc.value.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

        goaway = self._received_goaway(client)
        assert goaway.error_code == h2.errors.ErrorCodes.PROTOCOL_ERROR

    def test_server_priority_update_on_push_stream(self):
        """
        A server can reprioritize a pushed response: the client receives a
        PriorityUpdateReceived event on the push stream.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        server.push_stream(
            stream_id=1,
            promised_stream_id=2,
            request_headers=self.example_request_headers,
        )
        server.priority_update(2, urgency=5, incremental=True)
        events = client.receive_data(server.data_to_send())

        push_events = [
            event for event in events
            if isinstance(event, h2.events.PriorityUpdateReceived)
        ]
        assert len(push_events) == 1
        event = push_events[0]
        assert event.stream_id == 2
        assert event.prioritized_element_id == 2
        assert event.urgency == 5
        assert event.incremental is True

    def test_priority_update_for_idle_stream(self):
        """
        A PRIORITY_UPDATE for a stream that does not exist yet is passed
        through to the application without retaining any state for it.
        """
        client, server = self._connection_pair()

        client.priority_update(5, urgency=2)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.prioritized_element_id == 5
        assert event.urgency == 2

        # No stream or priority state is retained for the unknown stream.
        assert 5 not in server.streams

    def test_priority_update_for_closed_stream(self):
        """
        A PRIORITY_UPDATE for a closed stream is passed through to the
        application without error and without retaining state.
        """
        client, server = self._connection_pair()

        client.send_headers(1, self.example_request_headers, end_stream=True)
        server.receive_data(client.data_to_send())
        server.send_headers(1, self.example_response_headers, end_stream=True)
        client.receive_data(server.data_to_send())
        assert server.streams[1].closed

        client.priority_update(1, urgency=7, incremental=True)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.prioritized_element_id == 1
        assert event.urgency == 7
        assert event.incremental is True

    def test_rfc7540_priority_interface_still_works(self):
        """
        The old RFC 7540 priority interface is unaffected: PRIORITY frames
        still emit PriorityUpdated events.
        """
        client, server = self._connection_pair()
        self._open_stream(client, server)

        client.prioritize(1, weight=42, depends_on=0, exclusive=False)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdated)
        assert event.stream_id == 1
        assert event.weight == 42
