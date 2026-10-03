"""
Tests for the two-phase graceful shutdown support.

These tests validate that an endpoint (typically a server) can stop accepting
new streams while allowing in-flight streams to complete, using the pair of
GOAWAY frames described in RFC 7540 Section 6.8.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions


class TestGracefulShutdown:
    """
    Tests for graceful shutdown of a single connection.
    """

    server_config = h2.config.H2Configuration(client_side=False)
    client_config = h2.config.H2Configuration(client_side=True)

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    example_response_headers = [
        (":status", "200"),
        ("server", "fake-serv/0.1.0"),
    ]

    def _server_with_open_stream(self, frame_factory, end_stream=True):
        """
        Returns a server connection with stream 1 accepted from the client.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        f = frame_factory.build_headers_frame(
            stream_id=1,
            headers=self.example_request_headers,
            flags=["END_STREAM"] if end_stream else [],
        )
        c.receive_data(f.serialize())
        c.clear_outbound_data_buffer()
        return c

    def test_initiate_graceful_shutdown_emits_first_goaway(self, frame_factory) -> None:
        """
        The first phase of a graceful shutdown emits a GOAWAY frame with
        last_stream_id set to 2**31 - 1 and NO_ERROR, per RFC 7540 S6.8.
        """
        c = self._server_with_open_stream(frame_factory)

        c.initiate_graceful_shutdown()

        f = frame_factory.build_goaway_frame(
            last_stream_id=2**31 - 1,
            error_code=h2.errors.ErrorCodes.NO_ERROR,
        )
        assert c.data_to_send() == f.serialize()
        assert c.state_machine.state is h2.connection.ConnectionState.GRACEFUL_SHUTDOWN
        assert c.graceful_shutdown_in_progress

    def test_close_connection_completes_graceful_shutdown(self, frame_factory) -> None:
        """
        Calling close_connection during a graceful shutdown emits the final
        GOAWAY frame, carrying the actual last accepted stream ID.
        """
        c = self._server_with_open_stream(frame_factory)
        c.initiate_graceful_shutdown()
        c.clear_outbound_data_buffer()

        c.close_connection()

        f = frame_factory.build_goaway_frame(last_stream_id=1)
        assert c.data_to_send() == f.serialize()
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED
        assert not c.graceful_shutdown_in_progress

    def test_close_connection_immediate_semantics_unchanged(self, frame_factory) -> None:
        """
        Calling close_connection without a graceful shutdown first keeps its
        original semantics: a single immediate GOAWAY and a closed connection.
        """
        c = self._server_with_open_stream(frame_factory)

        c.close_connection()

        f = frame_factory.build_goaway_frame(last_stream_id=1)
        assert c.data_to_send() == f.serialize()
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED
        assert not c.graceful_shutdown_in_progress

        # The connection is closed immediately: existing streams cannot be
        # used anymore.
        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_data(stream_id=1, data=b"some data")

    def test_existing_streams_continue_during_graceful_shutdown(self, frame_factory) -> None:
        """
        During graceful shutdown, existing streams can still send and receive
        DATA, update flow control windows, and end.
        """
        c = self._server_with_open_stream(frame_factory, end_stream=False)
        c.initiate_graceful_shutdown()
        c.clear_outbound_data_buffer()

        # Inbound DATA on the existing stream is accepted.
        f = frame_factory.build_data_frame(data=b"hello", stream_id=1)
        events = c.receive_data(f.serialize())
        assert isinstance(events[0], h2.events.DataReceived)
        assert events[0].stream_id == 1

        # Flow control windows can still be updated in both directions.
        c.increment_flow_control_window(1024)
        f = frame_factory.build_window_update_frame(stream_id=0, increment=1024)
        c.receive_data(f.serialize())
        f = frame_factory.build_window_update_frame(stream_id=1, increment=1024)
        c.receive_data(f.serialize())

        # Outbound DATA on the existing stream is accepted.
        c.send_data(stream_id=1, data=b"world")

        # The stream can still be ended by both peers.
        f = frame_factory.build_data_frame(
            data=b"", stream_id=1, flags=["END_STREAM"],
        )
        events = c.receive_data(f.serialize())
        assert any(isinstance(e, h2.events.StreamEnded) for e in events)
        c.end_stream(stream_id=1)

        assert c.state_machine.state is h2.connection.ConnectionState.GRACEFUL_SHUTDOWN
        assert c.graceful_shutdown_in_progress

    def test_control_frames_handled_during_graceful_shutdown(self, frame_factory) -> None:
        """
        PING and SETTINGS frames are still exchanged during graceful shutdown.
        """
        c = self._server_with_open_stream(frame_factory)
        c.initiate_graceful_shutdown()
        c.clear_outbound_data_buffer()

        # PING frames are answered.
        f = frame_factory.build_ping_frame(ping_data=b"12345678")
        c.receive_data(f.serialize())
        expected = frame_factory.build_ping_frame(
            ping_data=b"12345678", flags=["ACK"],
        )
        assert c.data_to_send() == expected.serialize()
        c.ping(b"87654321")

        # SETTINGS frames are acknowledged, and we can still emit our own.
        f = frame_factory.build_settings_frame({})
        c.receive_data(f.serialize())
        c.update_settings({})

        assert c.state_machine.state is h2.connection.ConnectionState.GRACEFUL_SHUTDOWN

    def test_late_inbound_streams_refused_during_graceful_shutdown(self, frame_factory) -> None:
        """
        New inbound streams that arrive during graceful shutdown are refused
        with REFUSED_STREAM and are not counted as accepted.
        """
        c = self._server_with_open_stream(frame_factory)
        c.initiate_graceful_shutdown()
        c.clear_outbound_data_buffer()

        for stream_id in (3, 5):
            f = frame_factory.build_headers_frame(
                stream_id=stream_id,
                headers=self.example_request_headers,
                flags=["END_STREAM"],
            )
            events = c.receive_data(f.serialize())

            # The request is not surfaced to the application...
            assert not events

            # ...and the stream is refused at the boundary.
            expected = frame_factory.build_rst_stream_frame(
                stream_id=stream_id,
                error_code=h2.errors.ErrorCodes.REFUSED_STREAM,
            )
            assert c.data_to_send() == expected.serialize()

        # The refused streams were never accepted: the highest inbound
        # stream ID still reflects only the streams accepted before the
        # shutdown began.
        assert c.highest_inbound_stream_id == 1

    def test_new_outbound_streams_rejected_during_graceful_shutdown(self, frame_factory) -> None:
        """
        The local peer cannot open new streams while draining.
        """
        c = h2.connection.H2Connection(config=self.client_config)
        c.initiate_connection()
        c.send_headers(stream_id=1, headers=self.example_request_headers)
        c.clear_outbound_data_buffer()

        f = frame_factory.build_goaway_frame(last_stream_id=2**31 - 1)
        c.receive_data(f.serialize())
        assert c.graceful_shutdown_in_progress

        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_headers(stream_id=3, headers=self.example_request_headers)

    def test_receiving_goaway_with_open_streams_drains_instead_of_closing(self, frame_factory) -> None:
        """
        Receiving a GOAWAY frame while streams are open moves the connection
        to the draining state: buffered outbound data is preserved and
        existing streams can continue in both directions.
        """
        c = h2.connection.H2Connection(config=self.client_config)
        c.initiate_connection()
        c.send_headers(stream_id=1, headers=self.example_request_headers)
        c.send_data(stream_id=1, data=b"buffered")

        f = frame_factory.build_goaway_frame(last_stream_id=2**31 - 1)
        events = c.receive_data(f.serialize())

        assert isinstance(events[0], h2.events.ConnectionTerminated)
        assert c.state_machine.state is h2.connection.ConnectionState.GRACEFUL_SHUTDOWN

        # The outbound buffer was not discarded...
        buffered = c.data_to_send()
        assert buffered
        assert b"buffered" in buffered

        # ...inbound data on the existing stream is still accepted...
        f = frame_factory.build_headers_frame(
            stream_id=1,
            headers=self.example_response_headers,
            flags=["END_STREAM"],
        )
        events = c.receive_data(f.serialize())
        assert isinstance(events[0], h2.events.ResponseReceived)

        # ...and a subsequent GOAWAY frame completes the shutdown.
        f = frame_factory.build_goaway_frame(last_stream_id=1)
        events = c.receive_data(f.serialize())
        assert isinstance(events[0], h2.events.ConnectionTerminated)
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED

    def test_goaway_last_stream_id_must_not_increase(self, frame_factory) -> None:
        """
        A GOAWAY frame must not re-accept streams rejected by a previous
        GOAWAY frame: last_stream_id may not increase.
        """
        c = self._server_with_open_stream(frame_factory)

        c.close_connection(last_stream_id=1)
        c.close_connection(last_stream_id=1)
        c.close_connection(last_stream_id=0)

        with pytest.raises(h2.exceptions.ProtocolError):
            c.close_connection(last_stream_id=3)

    def test_final_goaway_defaults_to_accepted_streams(self, frame_factory) -> None:
        """
        After a graceful shutdown begins, the final GOAWAY frame defaults to
        the highest actually-accepted inbound stream ID.
        """
        c = self._server_with_open_stream(frame_factory)
        c.initiate_graceful_shutdown()
        c.clear_outbound_data_buffer()

        c.close_connection()

        f = frame_factory.build_goaway_frame(last_stream_id=1)
        assert c.data_to_send() == f.serialize()


class TestGracefulShutdownTwoStacks:
    """
    A two-ended simulation of a graceful server shutdown: one response is
    still in flight and one request arrives too late.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    example_response_headers = [
        (":status", "200"),
        ("server", "fake-serv/0.1.0"),
    ]

    def _connected_pair(self):
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
        server.receive_data(client.data_to_send())
        return client, server

    def test_graceful_shutdown_delivers_in_flight_and_rejects_late(self) -> None:
        """
        The unfinished response is delivered in full, while the late request
        is refused at the boundary of the final GOAWAY last_stream_id.
        """
        client, server = self._connected_pair()

        # One request whose response is still in flight.
        client.send_headers(
            stream_id=1, headers=self.example_request_headers, end_stream=True,
        )
        events = server.receive_data(client.data_to_send())
        assert isinstance(events[0], h2.events.RequestReceived)

        server.send_headers(stream_id=1, headers=self.example_response_headers)
        events = client.receive_data(server.data_to_send())
        assert isinstance(events[0], h2.events.ResponseReceived)

        # A second request races with the shutdown: it is in flight before
        # the client can see the server's first GOAWAY frame.
        client.send_headers(
            stream_id=3, headers=self.example_request_headers, end_stream=True,
        )
        late_request = client.data_to_send()

        # The server begins to shut down: first GOAWAY with 2**31 - 1.
        server.initiate_graceful_shutdown()
        events = client.receive_data(server.data_to_send())
        terminated = [
            e for e in events if isinstance(e, h2.events.ConnectionTerminated)
        ]
        assert len(terminated) == 1
        assert terminated[0].last_stream_id == 2**31 - 1
        assert terminated[0].error_code == h2.errors.ErrorCodes.NO_ERROR
        assert client.graceful_shutdown_in_progress

        # Having seen the GOAWAY, the client cannot open further streams.
        with pytest.raises(h2.exceptions.ProtocolError):
            client.send_headers(
                stream_id=5, headers=self.example_request_headers,
            )

        # The late request reaches the server after shutdown began: it is
        # refused and never surfaced to the application.
        events = server.receive_data(late_request)
        assert not [
            e for e in events if isinstance(e, h2.events.RequestReceived)
        ]
        assert server.highest_inbound_stream_id == 1
        events = client.receive_data(server.data_to_send())
        resets = [e for e in events if isinstance(e, h2.events.StreamReset)]
        assert len(resets) == 1
        assert resets[0].stream_id == 3
        assert resets[0].error_code == h2.errors.ErrorCodes.REFUSED_STREAM

        # The in-flight response is completed normally.
        server.send_data(stream_id=1, data=b"hello, world", end_stream=True)

        # The caller observes that the connection has drained and finishes
        # the shutdown with the final GOAWAY frame.
        assert server.open_inbound_streams == 0
        assert server.graceful_shutdown_in_progress
        server.close_connection()
        assert not server.graceful_shutdown_in_progress

        events = client.receive_data(server.data_to_send())

        # The response was delivered in full.
        data_events = [
            e for e in events if isinstance(e, h2.events.DataReceived)
        ]
        assert b"".join(e.data for e in data_events) == b"hello, world"
        assert any(
            isinstance(e, h2.events.StreamEnded) and e.stream_id == 1
            for e in events
        )

        # The final GOAWAY draws the boundary at stream 1: the late stream 3
        # falls outside it and was therefore never processed.
        terminated = [
            e for e in events if isinstance(e, h2.events.ConnectionTerminated)
        ]
        assert len(terminated) == 1
        assert terminated[0].last_stream_id == 1
        assert terminated[0].last_stream_id < 3

        assert client.state_machine.state is h2.connection.ConnectionState.CLOSED
        assert server.state_machine.state is h2.connection.ConnectionState.CLOSED
