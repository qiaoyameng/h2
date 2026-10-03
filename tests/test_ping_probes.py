from __future__ import annotations

import pytest

import h2.connection
import h2.events
import h2.exceptions
import h2.ping

from .helpers import FrameFactory


class TestPingProbes:
    @pytest.fixture
    def frame_factory(self) -> FrameFactory:
        return FrameFactory()

    def test_acks_arrive_out_of_order_and_timeout_ack_is_ignored(
        self, frame_factory,
    ) -> None:
        times = iter([
            10.0,
            11.0,
            12.0,
            12.5,
            13.5,
            22.0,
            22.5,
        ])
        connection = h2.connection.H2Connection(clock=lambda: next(times))
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())
        connection.clear_outbound_data_buffer()

        first = connection.ping(timeout=10)
        second = connection.ping(timeout=10)
        third = connection.ping(timeout=10)

        assert first is not None
        assert second is not None
        assert third is not None
        assert first.ping_data != second.ping_data != third.ping_data
        assert len({first.ping_data, second.ping_data, third.ping_data}) == 3
        assert not first.done
        assert not second.done
        assert not third.done

        third_events = connection.receive_data(
            frame_factory.build_ping_frame(third.ping_data, flags=["ACK"]).serialize(),
        )
        first_events = connection.receive_data(
            frame_factory.build_ping_frame(first.ping_data, flags=["ACK"]).serialize(),
        )

        assert third.status is h2.ping.PingProbeStatus.ACKNOWLEDGED
        assert third.rtt == pytest.approx(0.5)
        assert first.status is h2.ping.PingProbeStatus.ACKNOWLEDGED
        assert first.rtt == pytest.approx(3.5)
        assert not second.done

        timed_out = connection.advance_ping_timeouts()
        assert timed_out == [second]
        assert second.status is h2.ping.PingProbeStatus.TIMED_OUT
        assert second.rtt is None

        late_events = connection.receive_data(
            frame_factory.build_ping_frame(second.ping_data, flags=["ACK"]).serialize(),
        )
        duplicate_events = connection.receive_data(
            frame_factory.build_ping_frame(first.ping_data, flags=["ACK"]).serialize(),
        )

        assert second.status is h2.ping.PingProbeStatus.TIMED_OUT
        assert second.rtt is None
        assert first.status is h2.ping.PingProbeStatus.ACKNOWLEDGED
        assert first.rtt == pytest.approx(3.5)

        for events in (third_events, first_events, late_events, duplicate_events):
            assert len(events) == 1
            assert isinstance(events[0], h2.events.PingAckReceived)

    def test_unknown_ack_remains_event_without_probe(self, frame_factory) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        ping_data = b"01234567"
        events = connection.receive_data(
            frame_factory.build_ping_frame(ping_data, flags=["ACK"]).serialize(),
        )

        assert len(events) == 1
        assert events[0] == h2.events.PingAckReceived(ping_data=ping_data)

    def test_plain_ping_is_not_limited_or_tracked(self, frame_factory) -> None:
        connection = h2.connection.H2Connection(max_pending_pings=1)
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        assert connection.ping(b"01234567") is None
        probe = connection.ping(b"87654321", timeout=1, now=0)
        assert probe is not None
        assert connection.ping(b"76543210") is None
        assert connection.ping(b"76543210") is None

        with pytest.raises(ValueError, match="pending probe"):
            connection.ping(b"87654321")

        events = connection.receive_data(
            frame_factory.build_ping_frame(b"01234567", flags=["ACK"]).serialize(),
            now=0.5,
        )

        assert len(events) == 1
        assert not probe.done

    def test_probe_can_use_data_sent_by_plain_ping(self, frame_factory) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        ping_data = b"01234567"
        assert connection.ping(ping_data) is None
        probe = connection.ping(ping_data, timeout=1, now=0)

        assert probe is not None
        assert probe.ping_data == ping_data

    def test_pending_ping_limit_and_duplicate_data_are_rejected(
        self, frame_factory,
    ) -> None:
        connection = h2.connection.H2Connection(max_pending_pings=1)
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        ping_data = b"01234567"
        probe = connection.ping(ping_data, timeout=1, now=0)

        assert probe is not None
        with pytest.raises(ValueError, match="already in use"):
            connection.ping(ping_data, timeout=1, now=0)
        with pytest.raises(h2.exceptions.TooManyPingsError):
            connection.ping(timeout=1, now=0)

        connection.receive_data(
            frame_factory.build_ping_frame(ping_data, flags=["ACK"]).serialize(),
            now=0.5,
        )

        with pytest.raises(ValueError, match="previously used by a probe"):
            connection.ping(ping_data, timeout=1, now=1)

    @pytest.mark.parametrize(
        ("timeout", "advanced_time", "timed_out"),
        [
            (1, 1, False),
            (1, 1.000001, True),
        ],
    )
    def test_timeout_is_advanced_explicitly(
        self,
        frame_factory,
        timeout,
        advanced_time,
        timed_out,
    ) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        probe = connection.ping(timeout=timeout, now=0)
        assert probe is not None

        expired = connection.advance_ping_timeouts(advanced_time)

        assert (expired == [probe]) is timed_out
        if timed_out:
            assert probe.status is h2.ping.PingProbeStatus.TIMED_OUT
        else:
            assert not probe.done

    def test_probe_requires_monotonic_time_and_rejects_backwards_time(self) -> None:
        connection = h2.connection.H2Connection()

        with pytest.raises(ValueError, match="monotonic clock or explicit time"):
            connection.ping(timeout=1)

        probe = connection.ping(timeout=1, now=1)
        assert probe is not None
        with pytest.raises(ValueError, match="moved backwards"):
            connection.advance_ping_timeouts(0.5)

    def test_invalid_clock_and_pending_limit_are_rejected(self) -> None:
        with pytest.raises(TypeError, match="clock must be callable"):
            h2.connection.H2Connection(clock=object())
        with pytest.raises(ValueError, match="positive integer"):
            h2.connection.H2Connection(max_pending_pings=0)

    @pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True, "1"])
    def test_invalid_probe_timeout_is_rejected(self, timeout) -> None:
        connection = h2.connection.H2Connection()
        with pytest.raises(ValueError, match="positive finite"):
            connection.ping(timeout=timeout, now=0)

    def test_completion_is_one_shot_on_late_termination(self, frame_factory) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        probe = connection.ping(timeout=1, now=0)
        assert probe is not None
        assert connection.advance_ping_timeouts(1.1) == [probe]

        events = connection.receive_data(
            frame_factory.build_goaway_frame(last_stream_id=0).serialize(),
        )

        assert len(events) == 1
        assert probe.status is h2.ping.PingProbeStatus.TIMED_OUT

    def test_remote_termination_fails_remaining_probes(self, frame_factory) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        probe = connection.ping(timeout=10, now=0)
        assert probe is not None

        events = connection.receive_data(
            frame_factory.build_goaway_frame(last_stream_id=0).serialize(),
        )

        assert len(events) == 1
        assert isinstance(events[0], h2.events.ConnectionTerminated)
        assert probe.status is h2.ping.PingProbeStatus.CONNECTION_TERMINATED
        assert probe.rtt is None
        assert probe.done

    def test_local_termination_fails_remaining_probes(self, frame_factory) -> None:
        connection = h2.connection.H2Connection()
        connection.initiate_connection()
        connection.receive_data(frame_factory.build_settings_frame({}).serialize())

        probe = connection.ping(timeout=10, now=0)
        assert probe is not None

        connection.close_connection()

        assert probe.status is h2.ping.PingProbeStatus.CONNECTION_TERMINATED
        assert probe.rtt is None
        assert probe.done
