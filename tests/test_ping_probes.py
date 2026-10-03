from __future__ import annotations

import pytest

import h2.connection
import h2.events
import h2.exceptions

from . import helpers


def make_connection(max_pending_pings=8):
    now = 0.0

    def clock():
        return now

    connection = h2.connection.H2Connection(
        clock=clock,
        max_pending_pings=max_pending_pings,
    )
    connection.initiate_connection()
    return connection, lambda: now


def ack_frame(probe):
    return helpers.FrameFactory().build_ping_frame(
        probe.ping_data, flags=["ACK"],
    )


class TestPingProbes:
    def test_acks_can_arrive_out_of_order(self, frame_factory) -> None:
        connection, _ = make_connection()
        connection.clear_outbound_data_buffer()

        first = connection.ping(timeout=10)
        second = connection.ping(timeout=10)

        assert first is not None
        assert second is not None
        assert first.ping_data != second.ping_data

        connection.receive_data(ack_frame(second).serialize())
        connection.receive_data(ack_frame(first).serialize())

        assert second.completed is True
        assert second.result is True
        assert second.rtt == 0
        assert first.completed is True
        assert first.result is True
        assert first.rtt == 0

    def test_late_ack_after_timeout_does_not_complete_new_probe(self, frame_factory) -> None:
        connection, current_time = make_connection()
        connection.clear_outbound_data_buffer()

        first = connection.ping(timeout=1)
        second = connection.ping(timeout=10)
        third = connection.ping(timeout=10)
        assert first is not None
        assert second is not None
        assert third is not None

        connection.receive_data(ack_frame(third).serialize())
        timed_out = connection.advance_time(current_time() + 1)

        assert timed_out == [first]
        assert isinstance(first.error, h2.exceptions.PingProbeTimeoutError)
        assert first.completed is True
        assert first.result is False
        assert first.rtt is None
        assert third.completed is True
        assert third.result is True
        assert third.rtt == 0
        assert second.completed is False

        raw_late = connection.ping(first.ping_data)
        assert raw_late is None
        events = connection.receive_data(ack_frame(first).serialize())

        assert isinstance(events[0], h2.events.PingAckReceived)
        assert first.completed is True
        assert first.result is False
        assert isinstance(first.error, h2.exceptions.PingProbeTimeoutError)

        duplicate_events = connection.receive_data(ack_frame(third).serialize())
        assert isinstance(duplicate_events[0], h2.events.PingAckReceived)
        assert third.rtt == 0
        assert second.completed is False

    def test_probe_payloads_must_be_unique_while_pending(self) -> None:
        connection, _ = make_connection()
        connection.clear_outbound_data_buffer()

        first = connection.ping(timeout=10)
        assert first is not None

        with pytest.raises(ValueError, match="unique"):
            connection.ping(first.ping_data, timeout=10)

    def test_pending_probe_count_is_limited(self) -> None:
        connection, _ = make_connection(max_pending_pings=2)
        connection.clear_outbound_data_buffer()

        assert connection.ping(timeout=10) is not None
        assert connection.ping(timeout=10) is not None

        with pytest.raises(h2.exceptions.TooManyPingProbesError):
            connection.ping(timeout=10)

    def test_connection_termination_fails_remaining_probes(self, frame_factory) -> None:
        connection, _ = make_connection()
        connection.clear_outbound_data_buffer()

        first = connection.ping(timeout=10)
        second = connection.ping(timeout=10)
        assert first is not None
        assert second is not None

        frame = frame_factory.build_goaway_frame(last_stream_id=0)
        events = connection.receive_data(frame.serialize())

        assert isinstance(events[0], h2.events.ConnectionTerminated)
        for probe in (first, second):
            assert probe.completed is True
            assert probe.result is False
            assert isinstance(
                probe.error,
                h2.exceptions.PingProbeConnectionClosedError,
            )

        connection.receive_data(frame.serialize())
        assert first.result is False

    def test_normal_ping_events_remain_visible(self, frame_factory) -> None:
        connection, _ = make_connection()
        connection.clear_outbound_data_buffer()
        probe = connection.ping(timeout=10)
        assert probe is not None

        events = connection.receive_data(ack_frame(probe).serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.PingAckReceived)
        assert events[0].ping_data == probe.ping_data

        ping = frame_factory.build_ping_frame(b"12345678")
        events = connection.receive_data(ping.serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.PingReceived)
        assert events[0].ping_data == b"12345678"
