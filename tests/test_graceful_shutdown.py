from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings


class TestGracefulShutdown:
    server_config = h2.config.H2Configuration(client_side=False)
    client_config = h2.config.H2Configuration(client_side=True)

    request_headers = [
        (b":method", b"GET"),
        (b":path", b"/"),
        (b":authority", b"example.com"),
        (b":scheme", b"https"),
    ]

    response_headers = [
        (b":status", b"200"),
        (b"content-length", b"5"),
    ]

    def _exchange(
        self,
        client: h2.connection.H2Connection,
        server: h2.connection.H2Connection,
    ) -> None:
        client.initiate_connection()
        server.initiate_connection()
        server.receive_data(client.data_to_send())
        client.receive_data(server.data_to_send())
        server.receive_data(client.data_to_send())

    def test_two_phase_goaway_drains_existing_stream(self, frame_factory) -> None:
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)
        self._exchange(client, server)

        client.send_headers(1, self.request_headers, end_stream=True)
        events = server.receive_data(client.data_to_send())
        assert len(events) == 2
        assert isinstance(events[0], h2.events.RequestReceived)
        assert events[0].stream_id == 1
        assert isinstance(events[1], h2.events.StreamEnded)

        server.graceful_shutdown()
        first_goaway = server.data_to_send()

        client.send_headers(3, self.request_headers)
        late_request = client.data_to_send()

        events = client.receive_data(first_goaway)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.GracefulShutdownReceived)
        assert events[0].last_stream_id == 2**31 - 1
        assert client.is_draining
        assert server.is_draining
        assert client.remote_graceful_shutdown_active
        assert server.local_graceful_shutdown_active

        server.send_headers(1, self.response_headers)
        server.send_data(1, b"hello")
        events = client.receive_data(server.data_to_send())
        assert [type(event) for event in events] == [
            h2.events.ResponseReceived,
            h2.events.DataReceived,
        ]

        client.increment_flow_control_window(5, stream_id=1)
        client.increment_flow_control_window(5)
        events = server.receive_data(client.data_to_send())
        assert all(isinstance(event, h2.events.WindowUpdated) for event in events)

        client.ping(b"12345678")
        events = server.receive_data(client.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.PingReceived)
        events = client.receive_data(server.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.PingAckReceived)

        server.update_settings({h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 10})
        events = client.receive_data(server.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)
        events = server.receive_data(client.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.SettingsAcknowledged)

        events = server.receive_data(late_request)
        assert not events
        events = client.receive_data(server.data_to_send())
        assert len(events) == 1
        assert isinstance(events[0], h2.events.StreamReset)
        assert events[0].stream_id == 3
        assert events[0].error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert server.highest_inbound_stream_id == 1

        server.send_data(1, b"", end_stream=True)
        events = client.receive_data(server.data_to_send())
        assert isinstance(events[-1], h2.events.StreamEnded)
        assert events[-1].stream_id == 1
        assert server.open_inbound_streams == 0

        server.close_connection()
        final_goaway = server.data_to_send()
        expected_final_goaway = frame_factory.build_goaway_frame(
            last_stream_id=1,
        ).serialize()
        assert final_goaway == expected_final_goaway

        events = client.receive_data(final_goaway)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.ConnectionTerminated)
        assert events[0].last_stream_id == 1
        assert not client.is_draining
        assert not server.is_draining
        assert server.state_machine.state == h2.connection.ConnectionState.CLOSED

    def test_close_connection_retains_immediate_shutdown_semantics(self, frame_factory) -> None:
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)
        self._exchange(client, server)

        client.send_headers(1, self.request_headers, end_stream=True)
        server.receive_data(client.data_to_send())
        client.data_to_send()

        server.close_connection()
        expected = frame_factory.build_goaway_frame(last_stream_id=1).serialize()
        assert server.data_to_send() == expected
        assert not server.is_draining
        assert server.state_machine.state == h2.connection.ConnectionState.CLOSED

        with pytest.raises(h2.exceptions.ProtocolError):
            server.send_headers(1, self.response_headers)

    def test_final_goaway_cannot_claim_rejected_stream(self, frame_factory) -> None:
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)
        self._exchange(client, server)

        client.send_headers(1, self.request_headers, end_stream=True)
        server.receive_data(client.data_to_send())
        client.data_to_send()

        server.graceful_shutdown()
        client.receive_data(server.data_to_send())

        late_request = frame_factory.build_headers_frame(
            self.request_headers, stream_id=3,
        ).serialize()
        server.receive_data(late_request)
        client.receive_data(server.data_to_send())

        with pytest.raises(h2.exceptions.ProtocolError):
            server.close_connection(last_stream_id=3)

    def test_data_on_refused_late_stream_does_not_consume_window(self, frame_factory) -> None:
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)
        self._exchange(client, server)

        client.send_headers(1, self.request_headers, end_stream=True)
        server.receive_data(client.data_to_send())
        client.data_to_send()

        server.graceful_shutdown()
        client.receive_data(server.data_to_send())

        late_request = frame_factory.build_headers_frame(
            self.request_headers, stream_id=3,
        ).serialize()
        server.receive_data(late_request)
        client.receive_data(server.data_to_send())

        late_payload = b"x" * 16384
        late_data = frame_factory.build_data_frame(
            late_payload, stream_id=3,
        ).serialize()
        events = server.receive_data(late_data * 2)
        assert not events

        window_update = frame_factory.build_window_update_frame(
            stream_id=0, increment=len(late_payload) * 2,
        ).serialize()
        assert server.data_to_send() == window_update

    def test_local_graceful_shutdown_rejects_new_pushed_stream(self, frame_factory) -> None:
        client = h2.connection.H2Connection(config=self.client_config)
        server = h2.connection.H2Connection(config=self.server_config)
        self._exchange(client, server)

        client.send_headers(1, self.request_headers)
        server.receive_data(client.data_to_send())
        client.data_to_send()

        client.graceful_shutdown()
        assert client.data_to_send()

        push_promise = frame_factory.build_push_promise_frame(
            stream_id=1,
            promised_stream_id=2,
            headers=self.request_headers,
        )
        events = client.receive_data(push_promise.serialize())
        assert not events

        expected = frame_factory.build_rst_stream_frame(
            stream_id=2,
            error_code=h2.errors.ErrorCodes.REFUSED_STREAM,
        ).serialize()
        assert client.data_to_send() == expected
