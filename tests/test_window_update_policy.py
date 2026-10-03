"""
Tests for configurable inbound window update policies.
"""
from __future__ import annotations

from collections.abc import Callable

import pytest
from hyperframe.frame import Frame, RstStreamFrame, WindowUpdateFrame

import h2.config
import h2.connection
import h2.settings
import h2.windows
from .helpers import FrameFactory


class RecordingWindowUpdatePolicy:
    def __init__(self) -> None:
        self.calls = []

    def __call__(self, context: h2.windows.WindowUpdateContext) -> int | None:
        self.calls.append(context)
        call_count = sum(call.stream_id == context.stream_id for call in self.calls)
        if call_count <= 2:
            return min(context.consumed_bytes, 20 if context.stream_id else 25)
        return context.consumed_bytes


class PaddedWindowUpdatePolicy:
    def __init__(self) -> None:
        self.contexts = []

    def __call__(self, context: h2.windows.WindowUpdateContext) -> int | None:
        self.contexts.append(context)
        return context.consumed_bytes


class ResetWindowUpdatePolicy:
    def __init__(self) -> None:
        self.contexts = []

    def __call__(self, context: h2.windows.WindowUpdateContext) -> int | None:
        self.contexts.append(context)
        return context.consumed_bytes


def parse_frames(data: bytes) -> list[Frame]:
    frames = []
    while data:
        frame, body_length = Frame.parse_frame_header(memoryview(data[:9]))
        frame.parse_body(memoryview(data[9:9 + body_length]))
        frames.append(frame)
        data = data[9 + body_length:]
    return frames


def establish_connections(server_policy_factory: Callable[[], object]) -> tuple[
    h2.connection.H2Connection,
    h2.connection.H2Connection,
    FrameFactory,
]:
    frame_factory = FrameFactory()
    client = h2.connection.H2Connection()
    client.initiate_connection()

    server = h2.connection.H2Connection(
        config=h2.config.H2Configuration(
            client_side=False,
            inbound_window_update_policy=server_policy_factory,
        ),
    )
    server.receive_data(client.data_to_send())
    server.initiate_connection()
    client.receive_data(server.data_to_send())

    client.send_headers(1, [
        (":method", "POST"),
        (":path", "/"),
        (":authority", "example.com"),
        (":scheme", "https"),
    ])
    server.receive_data(client.data_to_send())
    return client, server, frame_factory


def test_policy_controls_connection_and_stream_frames_per_batch(frame_factory) -> None:
    client, server, frame_factory = establish_connections(
        RecordingWindowUpdatePolicy,
    )

    first_data = frame_factory.build_data_frame(b"x" * 100)
    server.receive_data(first_data.serialize())
    server.acknowledge_received_data(100, 1)

    first_updates = [
        frame for frame in parse_frames(server.data_to_send())
        if isinstance(frame, WindowUpdateFrame)
    ]
    assert [(frame.stream_id, frame.window_increment) for frame in first_updates] == [
        (0, 25),
        (1, 20),
    ]
    client.receive_data(b"".join(frame.serialize() for frame in first_updates))

    second_data = frame_factory.build_data_frame(b"y" * 100)
    server.receive_data(second_data.serialize())
    server.acknowledge_received_data(100, 1)

    second_updates = [
        frame for frame in parse_frames(server.data_to_send())
        if isinstance(frame, WindowUpdateFrame)
    ]
    assert [(frame.stream_id, frame.window_increment) for frame in second_updates] == [
        (0, 25),
        (1, 20),
    ]
    client.receive_data(b"".join(frame.serialize() for frame in second_updates))

    third_data = frame_factory.build_data_frame(b"z" * 200)
    server.receive_data(third_data.serialize())
    server.acknowledge_received_data(200, 1)

    final_updates = [
        frame for frame in parse_frames(server.data_to_send())
        if isinstance(frame, WindowUpdateFrame)
    ]
    assert [(frame.stream_id, frame.window_increment) for frame in final_updates] == [
        (0, 350),
        (1, 360),
    ]


def test_policy_receives_padded_data_flow_controlled_length(frame_factory) -> None:
    client, server, frame_factory = establish_connections(PaddedWindowUpdatePolicy)
    policy = server._inbound_window_update_policy

    data_frame = frame_factory.build_data_frame(b"abc", padding_len=10)
    server.receive_data(data_frame.serialize())
    server.acknowledge_received_data(14, 1)

    assert [(frame.stream_id, frame.window_increment) for frame in parse_frames(
        server.data_to_send(),
    ) if isinstance(frame, WindowUpdateFrame)] == [(0, 14), (1, 14)]
    assert [
        (context.stream_id, context.consumed_bytes, context.source_stream_id)
        for context in policy.contexts
    ] == [(0, 14, 1), (1, 14, None)]


def test_reset_stream_before_consumption_only_updates_connection(frame_factory) -> None:
    client, server, frame_factory = establish_connections(ResetWindowUpdatePolicy)

    client.send_data(1, b"x" * 100)
    client_data_to_send = client.data_to_send()
    client.reset_stream(1)
    reset_frames = [
        frame for frame in parse_frames(client.data_to_send())
        if isinstance(frame, RstStreamFrame)
    ]
    assert [(type(frame), frame.stream_id) for frame in reset_frames] == [
        (RstStreamFrame, 1),
    ]
    server.receive_data(
        client_data_to_send + b"".join(
            frame.serialize() for frame in reset_frames
        ),
    )
    server.acknowledge_received_data(100, 1)

    outbound_frames = parse_frames(server.data_to_send())
    assert [
        (type(frame), frame.stream_id, getattr(frame, "window_increment", None))
        for frame in outbound_frames
    ] == [(WindowUpdateFrame, 0, 100)]
    client.receive_data(b"".join(frame.serialize() for frame in outbound_frames))

    assert client.outbound_flow_control_window == 65535
    assert client._get_stream_by_id(1).outbound_flow_control_window == 65435


def test_initial_window_growth_is_visible_to_policy_before_consumption(frame_factory) -> None:
    config = h2.config.H2Configuration(
        client_side=False,
        inbound_window_update_policy=RecordingWindowUpdatePolicy,
    )
    server = h2.connection.H2Connection(config=config)
    server.initiate_connection()
    client = h2.connection.H2Connection()
    client.initiate_connection()
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())

    request = frame_factory.build_headers_frame([
        (":method", "POST"),
        (":path", "/"),
        (":authority", "example.com"),
        (":scheme", "https"),
    ])
    server.receive_data(request.serialize())
    stream = server._get_stream_by_id(1)

    server.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 100000})
    settings_ack = frame_factory.build_settings_frame({}, ack=True)
    server.receive_data(settings_ack.serialize())

    assert stream.inbound_flow_control_window == 100000
    assert stream._inbound_window_manager.max_window_size == 100000

    data_frame = frame_factory.build_data_frame(b"x" * 1000)
    server.receive_data(data_frame.serialize())
    server.acknowledge_received_data(1000, 1)

    policy = server._inbound_window_update_policy
    stream_context = next(
        context for context in policy.calls if context.stream_id == 1
    )
    assert (
        stream_context.consumed_bytes,
        stream_context.current_window_size,
        stream_context.max_window_size,
        stream_context.available_window_size,
        stream_context.source_stream_id,
    ) == (1000, 99000, 100000, 1000, None)
    assert stream.inbound_flow_control_window == 99020


def test_initial_window_shrink_is_visible_to_policy_before_consumption(frame_factory) -> None:
    config = h2.config.H2Configuration(
        client_side=False,
        inbound_window_update_policy=RecordingWindowUpdatePolicy,
    )
    server = h2.connection.H2Connection(config=config)
    client = h2.connection.H2Connection()
    client.initiate_connection()
    server.receive_data(client.data_to_send())
    server.initiate_connection()
    client.receive_data(server.data_to_send())

    request = frame_factory.build_headers_frame([
        (":method", "POST"),
        (":path", "/"),
        (":authority", "example.com"),
        (":scheme", "https"),
    ])
    server.receive_data(request.serialize())
    stream = server._get_stream_by_id(1)

    server.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1000})
    server.receive_data(frame_factory.build_settings_frame({}, ack=True).serialize())

    data_frame = frame_factory.build_data_frame(b"x" * 500)
    server.receive_data(data_frame.serialize())
    server.acknowledge_received_data(500, 1)

    policy = server._inbound_window_update_policy
    stream_context = next(
        context for context in policy.calls if context.stream_id == 1
    )
    assert (
        stream_context.consumed_bytes,
        stream_context.current_window_size,
        stream_context.max_window_size,
        stream_context.available_window_size,
        stream_context.source_stream_id,
    ) == (500, 500, 1000, 500, None)
    assert stream.inbound_flow_control_window == 520


@pytest.mark.parametrize(
    ("increment", "exception"),
    [(-1, ValueError), (2**31, OverflowError)],
)
def test_policy_suggestions_are_validated_for_invalid_ranges(increment, exception) -> None:
    manager = h2.windows.WindowManager(65535)
    manager.window_consumed(65535)

    def policy(context: h2.windows.WindowUpdateContext) -> int:
        return increment

    manager._window_update_policy = policy
    if increment > h2.windows.LARGEST_FLOW_CONTROL_WINDOW:
        manager._bytes_processed = h2.windows.LARGEST_FLOW_CONTROL_WINDOW
        with pytest.raises(exception):
            manager.process_bytes(1)
    else:
        with pytest.raises(exception):
            manager.process_bytes(65535)

    assert manager.current_window_size == 0
    if increment > h2.windows.LARGEST_FLOW_CONTROL_WINDOW:
        assert manager._bytes_processed == h2.windows.LARGEST_FLOW_CONTROL_WINDOW
    else:
        assert manager._bytes_processed == 0


def test_policy_cannot_return_more_than_consumed_bytes() -> None:
    manager = h2.windows.WindowManager(65535)
    manager.window_consumed(10)

    def policy(context: h2.windows.WindowUpdateContext) -> int:
        return 11

    manager._window_update_policy = policy
    with pytest.raises(ValueError):
        manager.process_bytes(10)

    assert manager.current_window_size == 65525
    assert manager._bytes_processed == 0


def test_policy_cannot_return_more_than_available_window() -> None:
    manager = h2.windows.WindowManager(5)
    manager.window_consumed(5)
    manager.max_window_size = 4

    def policy(context: h2.windows.WindowUpdateContext) -> int:
        return 5

    manager._window_update_policy = policy
    with pytest.raises(OverflowError):
        manager.process_bytes(5)

    assert manager.current_window_size == 0
    assert manager._bytes_processed == 0


def test_policy_factory_state_is_isolated_between_connections() -> None:
    class IsolationPolicy:
        def __init__(self) -> None:
            self.contexts = []

        def __call__(self, context: h2.windows.WindowUpdateContext) -> None:
            self.contexts.append(context)

    config = h2.config.H2Configuration(
        client_side=False,
        inbound_window_update_policy=IsolationPolicy,
    )
    first = h2.connection.H2Connection(config=config)
    second = h2.connection.H2Connection(config=config)

    assert first._inbound_window_update_policy is not second._inbound_window_update_policy

    first._inbound_flow_control_window_manager.process_bytes(1, source_stream_id=1)
    second._inbound_flow_control_window_manager.process_bytes(2, source_stream_id=3)

    assert [context.consumed_bytes for context in first._inbound_window_update_policy.contexts] == [1]
    assert [context.consumed_bytes for context in second._inbound_window_update_policy.contexts] == [2]


def test_policy_object_can_be_supplied_directly_and_config_rejects_non_callables() -> None:
    policy = RecordingWindowUpdatePolicy()
    config = h2.config.H2Configuration(inbound_window_update_policy=policy)
    connection = h2.connection.H2Connection(config=config)

    assert connection._inbound_window_update_policy is policy
    with pytest.raises(TypeError):
        h2.config.H2Configuration(inbound_window_update_policy="not-callable")
