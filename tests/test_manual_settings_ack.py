"""
Tests for the manual SETTINGS acknowledgement mode
(``H2Configuration(manual_settings_ack=True)``).

In this mode h2 does not automatically emit the SETTINGS acknowledgement
when a SETTINGS frame is received. Instead, the RemoteSettingsChanged event
carries an opaque ``ack_token`` and the application calls
``H2Connection.acknowledge_settings`` to emit the acknowledgement. The
settings values themselves take effect immediately upon receipt.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

from . import helpers


class TestManualSettingsAck:
    """
    Tests for manual acknowledgement of received SETTINGS frames.
    """

    server_config = h2.config.H2Configuration(
        client_side=False, manual_settings_ack=True,
    )
    client_config = h2.config.H2Configuration(
        client_side=True, manual_settings_ack=True,
    )

    request_headers = [
        (":method", "GET"),
        (":path", "/"),
        (":authority", "example.com"),
        (":scheme", "https"),
    ]

    def _server_with_settings(self, frame_factory, settings):
        """
        Build a server-side connection in manual ack mode that has received
        the client preamble and a single SETTINGS frame. Returns the
        connection and the RemoteSettingsChanged event.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_settings_frame(settings=settings)
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, h2.events.RemoteSettingsChanged)
        assert event.ack_token is not None
        return c, event

    def test_no_ack_emitted_until_acknowledge_settings(self, frame_factory) -> None:
        """
        No SETTINGS ACK is written to the output buffer until the
        application acknowledges the settings change.
        """
        c, event = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )

        # Nothing at all is emitted in response to the SETTINGS frame.
        assert c.data_to_send() == b""

        expected_frame = frame_factory.build_settings_frame(
            settings={}, ack=True,
        )
        c.acknowledge_settings(event.ack_token)
        assert c.data_to_send() == expected_frame.serialize()

    def test_settings_ack_frame_does_not_require_ack(self, frame_factory) -> None:
        """
        Receiving a SETTINGS ACK frame fires SettingsAcknowledged and does
        not itself require acknowledgement.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_settings_frame(settings={}, ack=True)
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)
        assert c.data_to_send() == b""
        assert not c._pending_settings_acks

    def test_window_shrink_applies_before_ack(self, frame_factory) -> None:
        """
        A reduced INITIAL_WINDOW_SIZE limits outbound data immediately,
        even though the SETTINGS ACK has not been emitted yet.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.receive_data(frame_factory.preamble())

        f = frame_factory.build_headers_frame(self.request_headers)
        c.receive_data(f.serialize())

        f = frame_factory.build_settings_frame(
            settings={h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 100},
        )
        events = c.receive_data(f.serialize())
        event = events[0]

        # The shrunk window is enforced before any acknowledgement.
        assert c.local_flow_control_window(1) == 100
        with pytest.raises(h2.exceptions.FlowControlError):
            c.send_data(1, b"x" * 200)

        c.acknowledge_settings(event.ack_token)
        assert c.local_flow_control_window(1) == 100

    def test_max_frame_size_applies_before_ack(self, frame_factory) -> None:
        """
        A changed MAX_FRAME_SIZE takes effect immediately, before the
        SETTINGS ACK is emitted.
        """
        c, event = self._server_with_settings(
            frame_factory,
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: 32768},
        )

        assert c.max_outbound_frame_size == 32768
        c.acknowledge_settings(event.ack_token)
        assert c.max_outbound_frame_size == 32768

    def test_consecutive_settings_acked_in_order(self, frame_factory) -> None:
        """
        Multiple received SETTINGS frames queue multiple tokens that must be
        acknowledged in receive order, emitting one ACK frame each.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        first = frame_factory.build_settings_frame(
            settings={h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 10},
        )
        second = frame_factory.build_settings_frame(
            settings={h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 20},
        )
        first_event = c.receive_data(first.serialize())[0]
        second_event = c.receive_data(second.serialize())[0]

        assert first_event.ack_token is not None
        assert second_event.ack_token is not None
        assert first_event.ack_token != second_event.ack_token

        # Out-of-order acknowledgement is rejected.
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(second_event.ack_token)
        assert c.data_to_send() == b""

        expected_frame = frame_factory.build_settings_frame(
            settings={}, ack=True,
        )
        expected_ack = expected_frame.serialize()

        c.acknowledge_settings(first_event.ack_token)
        assert c.data_to_send() == expected_ack
        c.acknowledge_settings(second_event.ack_token)
        assert c.data_to_send() == expected_ack

    def test_duplicate_ack_rejected(self, frame_factory) -> None:
        """
        Acknowledging the same settings change twice is rejected.
        """
        c, event = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )

        c.acknowledge_settings(event.ack_token)
        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(event.ack_token)

    def test_foreign_token_rejected(self, frame_factory) -> None:
        """
        A token issued by another connection is rejected.
        """
        c1, event1 = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )
        c2, event2 = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )

        assert event1.ack_token != event2.ack_token
        with pytest.raises(h2.exceptions.ProtocolError):
            c1.acknowledge_settings(event2.ack_token)
        with pytest.raises(h2.exceptions.ProtocolError):
            c2.acknowledge_settings(event1.ack_token)

        # Both connections can still acknowledge their own tokens.
        c1.acknowledge_settings(event1.ack_token)
        c2.acknowledge_settings(event2.ack_token)

    def test_unknown_token_rejected(self, frame_factory) -> None:
        """
        A token that was never issued is rejected.
        """
        c, event = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )

        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(event.ack_token + 1000)

    def test_close_with_unacknowledged_settings(self, frame_factory) -> None:
        """
        The connection can be closed while settings acknowledgements are
        still outstanding.
        """
        c, event = self._server_with_settings(
            frame_factory, helpers.SAMPLE_SETTINGS,
        )

        f = frame_factory.build_goaway_frame(last_stream_id=0)
        events = c.close_connection()
        assert not events
        assert c.data_to_send() == f.serialize()

    def test_automatic_mode_unchanged(self, frame_factory) -> None:
        """
        With the default configuration the ACK is emitted immediately, the
        event carries no token, and manual acknowledgement is rejected.
        """
        config = h2.config.H2Configuration(client_side=False)
        c = h2.connection.H2Connection(config=config)
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_settings_frame(
            settings=helpers.SAMPLE_SETTINGS,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert events[0].ack_token is None

        expected_frame = frame_factory.build_settings_frame(
            settings={}, ack=True,
        )
        assert c.data_to_send() == expected_frame.serialize()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.acknowledge_settings(1)

    def test_two_stacks_no_premature_ack(self, frame_factory) -> None:
        """
        End-to-end frame exchange: a manual-ack server does not write a
        SETTINGS ACK onto the wire until acknowledge_settings is called,
        and the ACK is then accepted by the peer.
        """
        client = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True),
        )
        server = h2.connection.H2Connection(config=self.server_config)

        client.initiate_connection()
        server.initiate_connection()
        server.clear_outbound_data_buffer()

        events = server.receive_data(client.data_to_send())
        settings_events = [
            e for e in events
            if isinstance(e, h2.events.RemoteSettingsChanged)
        ]
        assert len(settings_events) == 1
        token = settings_events[0].ack_token
        assert token is not None

        # The server has written nothing: no ACK has gone out early.
        assert server.data_to_send() == b""

        # The client's settings already apply on the server.
        assert (
            server.remote_settings.max_concurrent_streams
            == client.local_settings.max_concurrent_streams
        )

        server.acknowledge_settings(token)
        ack_data = server.data_to_send()
        expected_frame = frame_factory.build_settings_frame(
            settings={}, ack=True,
        )
        assert ack_data == expected_frame.serialize()

        # The client accepts the ACK for its own settings.
        events = client.receive_data(ack_data)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)

    def test_two_stacks_both_directions(self, frame_factory) -> None:
        """
        With manual ack enabled on both ends, each side acknowledges the
        other's settings independently and the exchange completes.
        """
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)

        client.initiate_connection()
        server.initiate_connection()

        server_events = server.receive_data(client.data_to_send())
        client_events = client.receive_data(server.data_to_send())

        client_token = next(
            e.ack_token for e in client_events
            if isinstance(e, h2.events.RemoteSettingsChanged)
        )
        server_token = next(
            e.ack_token for e in server_events
            if isinstance(e, h2.events.RemoteSettingsChanged)
        )

        client.acknowledge_settings(client_token)
        server.acknowledge_settings(server_token)

        server_events = server.receive_data(client.data_to_send())
        client_events = client.receive_data(server.data_to_send())

        assert any(
            isinstance(e, h2.events.SettingsAcknowledged)
            for e in server_events
        )
        assert any(
            isinstance(e, h2.events.SettingsAcknowledged)
            for e in client_events
        )
