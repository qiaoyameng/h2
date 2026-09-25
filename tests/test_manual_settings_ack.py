"""
Test manual acknowledgement of received SETTINGS frames.

When the ``manual_settings_ack`` configuration option is enabled, h2 defers
emission of SETTINGS acknowledgement frames: the application acknowledges
each received SETTINGS frame explicitly, in order, using the unique token
carried on the ``RemoteSettingsChanged`` event.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings

from . import helpers


def manual_server() -> h2.connection.H2Connection:
    config = h2.config.H2Configuration(
        client_side=False, manual_settings_ack=True,
    )
    return h2.connection.H2Connection(config=config)


def manual_client() -> h2.connection.H2Connection:
    config = h2.config.H2Configuration(
        client_side=True, manual_settings_ack=True,
    )
    return h2.connection.H2Connection(config=config)


class TestManualSettingsAck:
    """
    Tests for the manual settings acknowledgement mode.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]

    def receive_settings(self, conn, frame_factory, settings) -> list:
        """
        Feed a SETTINGS frame to a connection and return the emitted events.
        """
        f = frame_factory.build_settings_frame(settings=settings)
        return conn.receive_data(f.serialize())

    def test_no_ack_written_until_acknowledged(self, frame_factory) -> None:
        """
        In manual mode, two connected peers exchange frames and no SETTINGS
        ACK is emitted until acknowledge_settings is called.
        """
        client = h2.connection.H2Connection()
        server = manual_server()

        client.initiate_connection()
        server.initiate_connection()

        # The server's own preface (its SETTINGS frame) is drained first so
        # that anything emitted afterwards must be a response to the client.
        server_preface = server.data_to_send()

        # Exchange connection prefaces. The client automatically ACKs the
        # server's settings, but the server must not ACK anything yet.
        events = server.receive_data(client.data_to_send())
        settings_events = [
            e for e in events
            if isinstance(e, h2.events.RemoteSettingsChanged)
        ]
        assert len(settings_events) == 1
        assert settings_events[0].ack_token is not None
        assert server.data_to_send() == b""

        events = client.receive_data(server_preface)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)
        assert events[0].ack_token is None

        # The client's automatic ACK of the server settings is processed by
        # the server without needing any further acknowledgement.
        events = server.receive_data(client.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)
        assert server.data_to_send() == b""

        # The client changes its settings: still no ACK from the server.
        new_settings = {
            h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024,
            h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1024,
        }
        client.update_settings(new_settings)
        events = server.receive_data(client.data_to_send())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.RemoteSettingsChanged)
        assert event.ack_token is not None
        assert server.data_to_send() == b""

        expected_frame = frame_factory.build_settings_frame({}, ack=True)

        # Acknowledge the connection preface settings first: the newer
        # change cannot be acknowledged out of order.
        with pytest.raises(h2.exceptions.ProtocolError):
            server.acknowledge_settings(event.ack_token)
        server.acknowledge_settings(settings_events[0].ack_token)

        # Acknowledge the newer change: each acknowledgement emits exactly
        # one SETTINGS ACK frame, and the client processes both.
        server.acknowledge_settings(event.ack_token)

        ack_data = server.data_to_send()
        assert ack_data == expected_frame.serialize() * 2

        events = client.receive_data(ack_data)
        assert len(events) == 2
        assert isinstance(events[0], h2.events.SettingsAcknowledged)
        assert isinstance(events[1], h2.events.SettingsAcknowledged)
        assert (
            events[0].changed_settings[
                h2.settings.SettingCodes.HEADER_TABLE_SIZE
            ].new_value == 1024
        )
        assert not events[1].changed_settings

    def test_window_decrease_effective_before_ack(self, frame_factory) -> None:
        """
        A reduced INITIAL_WINDOW_SIZE limits outbound data immediately, even
        though the SETTINGS ACK has not been emitted yet.
        """
        c = manual_server()
        c.receive_data(frame_factory.preamble())
        c.receive_data(
            frame_factory.build_headers_frame(
                self.example_request_headers,
            ).serialize(),
        )

        assert c.local_flow_control_window(1) == 65535

        events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 100},
        )
        token = events[0].ack_token
        assert token is not None

        # The ACK is still pending, but the shrunk window already applies.
        assert c.local_flow_control_window(1) == 100
        with pytest.raises(h2.exceptions.FlowControlError):
            c.send_data(1, b"\x00" * 200)

        c.acknowledge_settings(token)
        assert c.local_flow_control_window(1) == 100

    def test_max_frame_size_effective_before_ack(self, frame_factory) -> None:
        """
        A changed MAX_FRAME_SIZE constrains outbound frames immediately, even
        though the SETTINGS ACK has not been emitted yet.
        """
        c = manual_server()
        c.receive_data(frame_factory.preamble())
        c.receive_data(
            frame_factory.build_headers_frame(
                self.example_request_headers,
            ).serialize(),
        )

        assert c.max_outbound_frame_size == 16384

        events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: 32768},
        )
        assert events[0].ack_token is not None

        # Before the ACK is emitted, larger frames are already allowed.
        assert c.max_outbound_frame_size == 32768
        c.send_data(1, b"\x00" * 20000)
        data = c.data_to_send()
        assert len(data) == 20000 + 9
        assert int.from_bytes(data[:3], "big") == 20000

    def test_consecutive_settings_acknowledged_in_order(
        self, frame_factory,
    ) -> None:
        """
        Multiple received SETTINGS frames queue up and must be acknowledged
        in the order they were received.
        """
        c = manual_server()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        first_events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024},
        )
        second_events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 2048},
        )

        first_token = first_events[0].ack_token
        second_token = second_events[0].ack_token
        assert first_token is not None
        assert second_token is not None
        assert first_token != second_token
        assert c.data_to_send() == b""

        # Acknowledging out of order is rejected.
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(second_token)
        assert c.data_to_send() == b""

        expected_ack = frame_factory.build_settings_frame(
            {}, ack=True,
        ).serialize()

        c.acknowledge_settings(first_token)
        assert c.data_to_send() == expected_ack

        c.acknowledge_settings(second_token)
        assert c.data_to_send() == expected_ack

    def test_duplicate_acknowledgement_rejected(self, frame_factory) -> None:
        """
        A token cannot be acknowledged twice.
        """
        c = manual_server()
        c.receive_data(frame_factory.preamble())

        events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024},
        )
        token = events[0].ack_token

        c.acknowledge_settings(token)
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(token)

    def test_foreign_and_unknown_tokens_rejected(self, frame_factory) -> None:
        """
        Tokens issued by another connection, and tokens that were never
        issued at all, are rejected.
        """
        c1 = manual_server()
        c2 = manual_server()
        c1.receive_data(frame_factory.preamble())
        c2.receive_data(frame_factory.preamble())

        events = self.receive_settings(
            c1, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024},
        )
        foreign_token = events[0].ack_token

        events = self.receive_settings(
            c2, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 2048},
        )
        own_token = events[0].ack_token

        # The other connection's token is not valid here.
        with pytest.raises(h2.exceptions.ProtocolError):
            c2.acknowledge_settings(foreign_token)

        # A token that was never issued is not valid either.
        with pytest.raises(h2.exceptions.ProtocolError):
            c2.acknowledge_settings(foreign_token + own_token + 1)

        # The connection's own token still works.
        c2.acknowledge_settings(own_token)
        expected_ack = frame_factory.build_settings_frame(
            {}, ack=True,
        ).serialize()
        assert c2.data_to_send() == expected_ack

    def test_settings_ack_frame_needs_no_acknowledgement(
        self, frame_factory,
    ) -> None:
        """
        Receiving a SETTINGS ACK frame does not itself create a pending
        acknowledgement.
        """
        c = manual_client()
        c.initiate_connection()
        c.update_settings({h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024})
        c.clear_outbound_data_buffer()

        f = frame_factory.build_settings_frame({}, ack=True)
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)
        assert c.data_to_send() == b""

        # Nothing is pending: any token is rejected.
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(1)

    def test_close_connection_with_unacknowledged_settings(
        self, frame_factory,
    ) -> None:
        """
        A connection can be closed while settings acknowledgements are still
        outstanding.
        """
        c = manual_server()
        c.receive_data(frame_factory.preamble())

        events = self.receive_settings(
            c, frame_factory,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 1024},
        )
        token = events[0].ack_token

        c.clear_outbound_data_buffer()
        c.close_connection()

        f = frame_factory.build_goaway_frame(last_stream_id=0)
        assert c.data_to_send() == f.serialize()

        # The connection is closed: the ACK can no longer be emitted.
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(token)

    def test_automatic_mode_is_unchanged(self, frame_factory) -> None:
        """
        With the default configuration, SETTINGS frames are acknowledged
        automatically and manual acknowledgement is rejected.
        """
        config = h2.config.H2Configuration(client_side=False)
        c = h2.connection.H2Connection(config=config)
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        events = self.receive_settings(
            c, frame_factory, helpers.SAMPLE_SETTINGS,
        )

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.RemoteSettingsChanged)
        assert event.ack_token is None

        expected_frame = frame_factory.build_settings_frame({}, ack=True)
        assert c.data_to_send() == expected_frame.serialize()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(1)
