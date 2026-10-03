"""
Tests for the budgeted inbound processing entry point,
``H2Connection.receive_data_budget``.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import ContinuationFrame, HeadersFrame

import h2.config
import h2.connection
import h2.events
import h2.exceptions


REQUEST_HEADERS = [
    (":method", "POST"),
    (":path", "/budget"),
    (":authority", "example.com"),
    (":scheme", "https"),
    ("content-length", "10"),
]

GET_HEADERS = [
    (":method", "GET"),
    (":path", "/other"),
    (":authority", "example.com"),
    (":scheme", "https"),
]


def _freeze(value):
    """
    Convert an event (or any nested value) into a comparable structure.
    """
    if isinstance(value, (bytes, str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_freeze(item) for item in value]
    if isinstance(value, dict):
        return {key: _freeze(item) for key, item in value.items()}
    if hasattr(value, "__dict__"):
        return (type(value).__name__, _freeze(vars(value)))
    return repr(value)


def _freeze_events(events):
    return [(type(event).__name__, _freeze(vars(event))) for event in events]


def _server_connection():
    config = h2.config.H2Configuration(client_side=False)
    conn = h2.connection.H2Connection(config=config)
    conn.initiate_connection()
    return conn


def _chunk(data, size):
    return [data[i:i + size] for i in range(0, len(data), size)]


def _build_interleaved_input(frame_factory):
    """
    Build a byte stream of interleaved SETTINGS, DATA, and a header block
    split across HEADERS and CONTINUATION frames.
    """
    encoded = frame_factory.encoder.encode(REQUEST_HEADERS)
    first = encoded[:len(encoded) // 3]
    second = encoded[len(encoded) // 3:2 * len(encoded) // 3]
    third = encoded[2 * len(encoded) // 3:]

    headers_frame = HeadersFrame(1)
    headers_frame.data = first

    first_continuation = ContinuationFrame(1)
    first_continuation.data = second

    second_continuation = ContinuationFrame(1)
    second_continuation.data = third
    second_continuation.flags.add("END_HEADERS")

    frames = [
        frame_factory.build_settings_frame(
            {
                1: 4096,   # HEADER_TABLE_SIZE
                2: 1,      # ENABLE_PUSH
                4: 65536,  # INITIAL_WINDOW_SIZE
            }
        ),
        headers_frame,
        first_continuation,
        second_continuation,
        frame_factory.build_data_frame(b"hello", stream_id=1),
        frame_factory.build_settings_frame({4: 131072, 3: 100}),
        frame_factory.build_data_frame(
            b"world", flags=["END_STREAM"], stream_id=1,
        ),
        frame_factory.build_headers_frame(
            GET_HEADERS, flags=["END_STREAM"], stream_id=3,
        ),
    ]

    return frame_factory.preamble() + b"".join(f.serialize() for f in frames)


def _run_reference(data):
    conn = _server_connection()
    events = conn.receive_data(data)
    return events, conn.data_to_send()


def _run_budgeted(chunks, budget, drain_each_chunk):
    conn = _server_connection()
    events = []
    for chunk in chunks:
        new_events, has_more = conn.receive_data_budget(chunk, budget)
        events.extend(new_events)
        while drain_each_chunk and has_more:
            new_events, has_more = conn.receive_data_budget(b"", budget)
            events.extend(new_events)

    # Drain whatever is left buffered.
    while True:
        new_events, has_more = conn.receive_data_budget(b"", budget)
        events.extend(new_events)
        if not has_more:
            break

    return events, conn.data_to_send()


class TestReceiveDataBudgetEquivalence:
    """
    The same interleaved input, processed with different network chunkings
    and frame budgets, must produce identical events and outbound bytes.
    """

    @pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 16, 64, 4096])
    @pytest.mark.parametrize("budget", [1, 2, 3, 7, 1000])
    @pytest.mark.parametrize("drain_each_chunk", [True, False])
    def test_interleaved_input_matches_unbudgeted(
        self, frame_factory, chunk_size, budget, drain_each_chunk,
    ):
        data = _build_interleaved_input(frame_factory)

        reference_events, reference_outbound = _run_reference(data)
        events, outbound = _run_budgeted(
            _chunk(data, chunk_size), budget, drain_each_chunk,
        )

        assert _freeze_events(events) == _freeze_events(reference_events)
        assert outbound == reference_outbound

    def test_interleaved_input_events_are_meaningful(self, frame_factory):
        """
        Sanity check: the interleaved input actually exercises SETTINGS,
        DATA, and multi-frame header blocks.
        """
        data = _build_interleaved_input(frame_factory)
        events, _ = _run_reference(data)
        event_types = [type(event) for event in events]

        assert h2.events.RemoteSettingsChanged in event_types
        assert h2.events.RequestReceived in event_types
        assert h2.events.DataReceived in event_types
        assert h2.events.StreamEnded in event_types
        assert event_types.count(h2.events.RemoteSettingsChanged) == 2
        assert event_types.count(h2.events.RequestReceived) == 2


class TestReceiveDataBudgetBehaviour:
    def test_continuation_frames_count_against_budget(
        self, frame_factory,
    ):
        conn = _server_connection()
        conn.receive_data(
            frame_factory.preamble() +
            frame_factory.build_settings_frame({}).serialize()
        )
        conn.clear_outbound_data_buffer()

        encoded = frame_factory.encoder.encode(REQUEST_HEADERS)
        headers_frame = HeadersFrame(1)
        headers_frame.data = encoded[:5]

        first_continuation = ContinuationFrame(1)
        first_continuation.data = encoded[5:10]

        second_continuation = ContinuationFrame(1)
        second_continuation.data = encoded[10:]
        second_continuation.flags.add("END_HEADERS")

        block = b"".join(
            f.serialize()
            for f in (headers_frame, first_continuation, second_continuation)
        )

        # The HEADERS frame consumes the whole budget: no event yet, but a
        # complete frame remains buffered.
        events, has_more = conn.receive_data_budget(block, budget=1)
        assert events == []
        assert has_more is True

        # The first CONTINUATION also counts against the budget.
        events, has_more = conn.receive_data_budget(b"", budget=1)
        assert events == []
        assert has_more is True

        # The final CONTINUATION completes the header block and emits the
        # request.
        events, has_more = conn.receive_data_budget(b"", budget=1)
        assert has_more is False
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RequestReceived)
        assert events[0].headers == [
            (name.encode(), value.encode())
            for name, value in REQUEST_HEADERS
        ]

    def test_partial_frame_is_preserved_between_calls(self, frame_factory):
        conn = _server_connection()
        conn.receive_data(
            frame_factory.preamble() +
            frame_factory.build_settings_frame({}).serialize()
        )
        headers = frame_factory.build_headers_frame(
            REQUEST_HEADERS, stream_id=1,
        )
        conn.receive_data(headers.serialize())

        data_frame = frame_factory.build_data_frame(
            b"helloworld", flags=["END_STREAM"], stream_id=1,
        ).serialize()

        # Feed the DATA frame one byte at a time: nothing may be emitted
        # until the final byte arrives, and no state may be lost.
        for byte in data_frame[:-1]:
            events, has_more = conn.receive_data_budget(bytes([byte]), 100)
            assert events == []
            assert has_more is False

        events, has_more = conn.receive_data_budget(data_frame[-1:], 100)
        assert has_more is False
        assert isinstance(events[0], h2.events.DataReceived)
        assert events[0].data == b"helloworld"
        assert isinstance(events[1], h2.events.StreamEnded)

    def test_preamble_split_across_calls(self, frame_factory):
        conn = _server_connection()
        preamble = frame_factory.preamble()
        settings = frame_factory.build_settings_frame({}).serialize()

        events, has_more = conn.receive_data_budget(preamble[:5], 100)
        assert events == []
        assert has_more is False

        events, has_more = conn.receive_data_budget(
            preamble[5:] + settings, 100,
        )
        assert has_more is False
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)

    def test_budget_zero_buffers_without_processing(self, frame_factory):
        conn = _server_connection()
        settings = frame_factory.build_settings_frame({}).serialize()
        data = frame_factory.preamble() + settings

        events, has_more = conn.receive_data_budget(data, budget=0)
        assert events == []
        assert has_more is True

        events, has_more = conn.receive_data_budget(b"", budget=1)
        assert has_more is False
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)

    def test_negative_budget_rejected(self, frame_factory):
        conn = _server_connection()
        with pytest.raises(ValueError, match="budget"):
            conn.receive_data_budget(b"", budget=-1)

    def test_has_more_reports_pending_frames(self, frame_factory):
        conn = _server_connection()
        data = (
            frame_factory.preamble() +
            frame_factory.build_settings_frame({}).serialize() +
            frame_factory.build_ping_frame(b"12345678").serialize()
        )

        events, has_more = conn.receive_data_budget(data, budget=1)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)
        assert has_more is True

        events, has_more = conn.receive_data_budget(b"", budget=1)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.PingReceived)
        assert has_more is False

    def test_buffer_limit_rejects_excess_data(self, frame_factory):
        conn = _server_connection()

        # The preamble is consumed on receipt and does not remain buffered.
        preamble = frame_factory.preamble()
        conn.receive_data_budget(preamble, budget=100)

        conn.max_inbound_buffer_size = 16

        with pytest.raises(h2.exceptions.InboundBufferOverflowError):
            conn.receive_data_budget(b"\x00" * 17, budget=100)

        # The failed call left the buffer untouched, and the connection is
        # still usable.
        assert len(conn.incoming_buffer) == 0
        settings = frame_factory.build_settings_frame({}).serialize()
        events, has_more = conn.receive_data_budget(settings, budget=1)
        assert has_more is False
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)

    def test_buffer_limit_counts_unprocessed_bytes(self, frame_factory):
        conn = _server_connection()
        settings = frame_factory.build_settings_frame({}).serialize()
        conn.receive_data_budget(frame_factory.preamble(), budget=100)

        conn.max_inbound_buffer_size = len(settings)
        events, has_more = conn.receive_data_budget(settings, budget=0)
        assert events == []
        assert has_more is True

        # One more byte than the limit allows is refused.
        with pytest.raises(h2.exceptions.InboundBufferOverflowError):
            conn.receive_data_budget(b"\x00", budget=0)

        # Draining the buffer makes room again.
        events, has_more = conn.receive_data_budget(b"", budget=1)
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)
        assert has_more is False

    def test_receive_data_ignores_buffer_limit(self, frame_factory):
        """
        The unbudgeted ``receive_data`` entry point keeps its historical
        behaviour and is not subject to the inbound buffer limit.
        """
        conn = _server_connection()
        conn.max_inbound_buffer_size = 1

        data = (
            frame_factory.preamble() +
            frame_factory.build_settings_frame({}).serialize()
        )
        events = conn.receive_data(data)
        assert len(events) == 1
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)

    def test_budgeted_and_unbudgeted_calls_interoperate(self, frame_factory):
        conn = _server_connection()
        data = (
            frame_factory.preamble() +
            frame_factory.build_settings_frame({}).serialize() +
            frame_factory.build_ping_frame(b"12345678").serialize()
        )

        events, has_more = conn.receive_data_budget(data[:30], budget=100)
        assert events == []
        assert has_more is False

        # The remainder can be picked up by plain receive_data.
        events = conn.receive_data(data[30:])
        assert [type(event) for event in events] == [
            h2.events.RemoteSettingsChanged,
            h2.events.PingReceived,
        ]
