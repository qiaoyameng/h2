"""
Tests for bounded processing of received data.
"""
from __future__ import annotations

import pytest

import h2
from h2.connection import ReceiveDataResult


def split_bytes(data: bytes, positions: list[int]) -> list[bytes]:
    return [data[start:end] for start, end in zip([0, *positions], [*positions, len(data)])]


def drain_budgeted(
    connection: h2.connection.H2Connection,
    chunks: list[bytes],
    budget: int,
) -> tuple[list[h2.events.Event], bytes]:
    events: list[h2.events.Event] = []

    for index, chunk in enumerate(chunks):
        result = connection.receive_data(chunk, frame_budget=budget)
        events.extend(result.events)

        while result.has_processable_data:
            result = connection.advance_inbound(budget)
            events.extend(result.events)

        if index < len(chunks) - 1:
            assert result.needs_more_data

    outbound = connection.data_to_send()
    return events, outbound


def event_signature(event: h2.events.Event) -> tuple[object, ...]:
    if isinstance(event, h2.events.RemoteSettingsChanged):
        return (
            type(event),
            tuple(
                (
                    setting,
                    change.original_value,
                    change.new_value,
                )
                for setting, change in event.changed_settings.items()
            ),
        )
    if isinstance(event, (h2.events.RequestReceived, h2.events.ResponseReceived)):
        return type(event), event.stream_id, event.headers
    if isinstance(event, h2.events.DataReceived):
        return (
            type(event),
            event.stream_id,
            event.data,
            event.flow_controlled_length,
        )
    return type(event), getattr(event, "stream_id", None)


@pytest.mark.parametrize("chunk_positions", [[], [5], [12, 31], [3, 17, 29, 44]])
def test_budgeted_receive_matches_unbounded_across_chunks(frame_factory, chunk_positions):
    frames = [
        frame_factory.build_settings_frame({}),
    ]

    headers = frame_factory.build_headers_frame(
        [
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b":scheme", b"https"),
            (b":method", b"GET"),
        ],
        flags=["END_STREAM"],
    )
    block = headers.data
    headers.data = block[:6]
    headers.flags.discard("END_HEADERS")
    continuation = frame_factory.build_continuation_frame(
        block[6:],
        flags=["END_HEADERS"],
    )
    frames.extend((
        headers,
        continuation,
        frame_factory.build_data_frame(b"hello", flags=["END_STREAM"]),
    ))

    data = b"".join(frame.serialize() for frame in frames)
    chunks = split_bytes(data, chunk_positions)

    reference = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    reference.initiate_connection()
    reference.receive_data(frame_factory.preamble())
    reference.clear_outbound_data_buffer()
    expected_events = reference.receive_data(data)
    expected_outbound = reference.data_to_send()

    budgeted = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    budgeted.initiate_connection()
    budgeted.receive_data(frame_factory.preamble())
    budgeted.clear_outbound_data_buffer()
    events, outbound = drain_budgeted(budgeted, chunks, budget=2)

    assert [event_signature(event) for event in events] == [
        event_signature(event) for event in expected_events
    ]
    assert outbound == expected_outbound
    assert budgeted.incoming_buffer.buffered_data_length == 0


def test_continuation_frames_consume_budget_before_event(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())
    connection.clear_outbound_data_buffer()

    headers = frame_factory.build_headers_frame(
        [
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b":scheme", b"https"),
            (b":method", b"GET"),
        ],
        flags=["END_STREAM"],
    )
    block = headers.data
    headers.data = block[:6]
    headers.flags.discard("END_HEADERS")
    continuation = frame_factory.build_continuation_frame(block[6:], flags=["END_HEADERS"])

    first = connection.receive_data(headers.serialize(), frame_budget=1)
    assert isinstance(first, ReceiveDataResult)
    assert first.events == []
    assert first.frames_processed == 1
    assert not first.has_processable_data
    assert first.needs_more_data

    second = connection.receive_data(continuation.serialize(), frame_budget=1)
    assert len(second.events) == 2
    assert second.frames_processed == 1
    assert not second.has_processable_data
    assert not second.needs_more_data


def test_budget_can_stop_with_complete_frame_retained(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())
    connection.clear_outbound_data_buffer()

    headers = frame_factory.build_headers_frame(
        [
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b":scheme", b"https"),
            (b":method", b"GET"),
        ],
    )
    data = headers.serialize() + frame_factory.build_data_frame(b"x").serialize()

    result = connection.receive_data(data, frame_budget=1)

    assert result.frames_processed == 1
    assert result.has_processable_data
    assert not result.needs_more_data

    result = connection.advance_inbound(frame_budget=1)
    assert result.frames_processed == 1
    assert not result.has_processable_data
    assert not result.needs_more_data


def test_zero_frame_budget_reports_retained_complete_frame(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())

    first = connection.receive_data(
        frame_factory.build_settings_frame({}).serialize(),
        frame_budget=0,
    )

    assert first.frames_processed == 0
    assert first.events == []
    assert first.has_processable_data
    assert not first.needs_more_data


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"frame_budget": -1}, ValueError),
        ({"frame_budget": 1.5}, TypeError),
        ({"frame_budget": True}, TypeError),
        ({"frame_budget": None, "max_inbound_buffer_size": 1}, ValueError),
        ({"frame_budget": 1, "max_inbound_buffer_size": -1}, ValueError),
        ({"frame_budget": 1, "max_inbound_buffer_size": 1.5}, TypeError),
    ],
)
def test_budgeted_receive_validates_arguments(frame_factory, kwargs, error):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())

    with pytest.raises(error):
        connection.receive_data(b"", **kwargs)


@pytest.mark.parametrize("preamble_size", [0, 4, 20, 24])
def test_half_frame_and_partial_preamble_are_retained(frame_factory, preamble_size):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()

    first_part = frame_factory.preamble()[:preamble_size]
    if first_part:
        result = connection.receive_data(first_part, frame_budget=2)

        assert result.frames_processed == 0
        assert result.events == []
        assert result.needs_more_data == (preamble_size < len(frame_factory.preamble()))
        assert not result.has_processable_data
    elif preamble_size == 0:
        result = connection.receive_data(b"", frame_budget=2)

        assert result.frames_processed == 0
        assert result.events == []
        assert result.needs_more_data
        assert not result.has_processable_data

    settings = frame_factory.build_settings_frame(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100},
    )
    remaining = frame_factory.preamble()[preamble_size:] + settings.serialize()[:12]
    result = connection.receive_data(remaining, frame_budget=2)

    assert result.frames_processed == 0
    assert result.needs_more_data
    assert not result.has_processable_data

    result = connection.receive_data(settings.serialize()[12:], frame_budget=2)
    assert result.frames_processed == 1
    assert isinstance(result.events[0], h2.events.RemoteSettingsChanged)
    assert not result.needs_more_data


def test_inbound_buffer_limit_is_enforced_without_mutating_buffer(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())
    connection.clear_outbound_data_buffer()

    frame = frame_factory.build_settings_frame(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100},
    )
    retained = frame.serialize()[:10]
    connection.receive_data(retained, frame_budget=1, max_inbound_buffer_size=20)

    valid_data = frame_factory.build_data_frame(b"x" * 10, stream_id=1).serialize()
    with pytest.raises(h2.exceptions.InboundBufferTooLargeError):
        connection.receive_data(valid_data[:14], frame_budget=1, max_inbound_buffer_size=20)

    assert connection.incoming_buffer.buffered_data_length == 10
    assert connection.data_to_send() == b""


def test_inbound_buffer_limit_is_enforced_when_advancing(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())
    connection.clear_outbound_data_buffer()

    frame = frame_factory.build_settings_frame(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100},
    )
    retained = frame.serialize()[:12]
    connection.receive_data(retained)

    with pytest.raises(h2.exceptions.InboundBufferTooLargeError):
        connection.advance_inbound(1, max_inbound_buffer_size=10)

    assert connection.incoming_buffer.buffered_data_length == 12
    assert connection.data_to_send() == b""


def test_receive_data_without_budget_remains_unbounded_and_backwards_compatible(frame_factory):
    connection = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    connection.initiate_connection()
    connection.receive_data(frame_factory.preamble())
    connection.clear_outbound_data_buffer()

    events = connection.receive_data(
        frame_factory.build_settings_frame({}).serialize()
        + frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100},
        ).serialize(),
    )

    assert len(events) == 2
    assert connection.incoming_buffer.max_buffer_size is None
