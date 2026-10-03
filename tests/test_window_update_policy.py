"""
test_window_update_policy
~~~~~~~~~~~~~~~~~~~~~~~~~

Tests for the configurable window update policy: the policy interface on
:class:`h2.windows.WindowManager`, validation of the values policies suggest,
and per-frame WINDOW_UPDATE accounting using two communicating
:class:`h2.connection.H2Connection` objects.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import (
    DataFrame,
    RstStreamFrame,
    WindowUpdateFrame,
)

import h2.config
import h2.connection
import h2.exceptions
import h2.settings
import h2.windows
from h2.frame_buffer import FrameBuffer

LARGEST_FLOW_CONTROL_WINDOW = h2.windows.LARGEST_FLOW_CONTROL_WINDOW


def parse_frames(data: bytes) -> list:
    """Parse a buffer of wire data into hyperframe frame objects."""
    buffer = FrameBuffer()
    buffer.add_data(data)
    buffer.max_frame_size = 1 << 24
    return list(buffer)


def window_updates(data: bytes) -> dict[int, list[int]]:
    """Map stream ID (0 for connection) to the increments of WINDOW_UPDATEs."""
    updates: dict[int, list[int]] = {}
    for frame in parse_frames(data):
        if isinstance(frame, WindowUpdateFrame):
            updates.setdefault(frame.stream_id, []).append(
                frame.window_increment,
            )
    return updates


class GreedyPolicy:
    """
    Replenishes the window by every byte that is released, immediately.

    Records every decision so tests can assert on the state handed to the
    policy.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[int, h2.windows.WindowState]] = []

    def window_increment(self, consumed_size, state):
        self.calls.append((consumed_size, state))
        return state.bytes_processed


class ThresholdPolicy:
    """Replenishes only once at least ``threshold`` bytes are in the bank."""

    def __init__(self, threshold: int) -> None:
        self.threshold = threshold

    def window_increment(self, consumed_size, state):
        if state.bytes_processed >= self.threshold:
            return state.bytes_processed
        return None


class ScriptedPolicy:
    """Returns a fixed list of scripted responses, repeating the last one."""

    def __init__(self, responses: list) -> None:
        self.responses = list(responses)
        self.index = 0

    def window_increment(self, consumed_size, state):
        response = self.responses[self.index]
        if self.index < len(self.responses) - 1:
            self.index += 1
        return response


class Pair:
    """A client and a server H2Connection, with their handshakes exchanged."""

    request_headers = [
        (":method", "GET"),
        (":path", "/"),
        (":authority", "example.com"),
        (":scheme", "https"),
    ]
    response_headers = [
        (b":status", b"200"),
        (b"server", b"test-server/0.1.0"),
        (b"content-length", b"0"),
    ]

    def __init__(
        self,
        client_policy=None,
        server_policy=None,
        initial_window_size: int | None = None,
    ) -> None:
        client_config = h2.config.H2Configuration(
            client_side=True,
            window_update_policy=client_policy,
        )
        server_config = h2.config.H2Configuration(
            client_side=False,
            window_update_policy=server_policy,
        )
        self.client = h2.connection.H2Connection(config=client_config)
        self.server = h2.connection.H2Connection(config=server_config)

        self.client.initiate_connection()
        self.server.initiate_connection()
        client_data = self.client.data_to_send()
        server_data = self.server.data_to_send()
        self.server.receive_data(client_data)
        self.client.receive_data(server_data)
        # Drain the ACKs both sides now have queued.
        self._exchange()

        if initial_window_size is not None:
            self.client.update_settings(
                {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: initial_window_size},
            )
            self._exchange()
        # The server advertises a large MAX_FRAME_SIZE so padding tests can
        # send big DATA frames in one piece.
        self.server.update_settings(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: 1 << 20},
        )
        self._exchange()

    def _exchange(self) -> None:
        """Move whatever is queued on each side over to the other side."""
        while True:
            client_data = self.client.data_to_send()
            server_data = self.server.data_to_send()
            if not client_data and not server_data:
                break
            if server_data:
                self.client.receive_data(server_data)
            if client_data:
                self.server.receive_data(client_data)

    def drain(self) -> tuple[bytes, bytes]:
        """Exchange all queued data; return all (client_bytes, server_bytes)."""
        client_parts: list[bytes] = []
        server_parts: list[bytes] = []
        while True:
            client_data = self.client.data_to_send()
            server_data = self.server.data_to_send()
            if not client_data and not server_data:
                break
            client_parts.append(client_data)
            server_parts.append(server_data)
            if server_data:
                self.client.receive_data(server_data)
            if client_data:
                self.server.receive_data(client_data)
        return b"".join(client_parts), b"".join(server_parts)

    def open_stream(self, stream_id: int = 1) -> None:
        self.client.send_headers(stream_id, self.request_headers)
        self.drain()

    def send_client_data(
        self,
        data: bytes,
        stream_id: int = 1,
        end_stream: bool = False,
        pad_length=None,
    ) -> tuple[bytes, bytes]:
        """Client sends DATA; server receives it. Returns the wire bytes."""
        self.client.send_data(
            stream_id, data, end_stream=end_stream, pad_length=pad_length,
        )
        return self.drain()


class TestWindowManagerPolicy:
    """Unit tests for the configurable policy hook on WindowManager."""

    def test_default_policy_matches_legacy_half_window(self) -> None:
        manager = h2.windows.WindowManager(1000)
        manager.window_consumed(600)
        assert manager.process_bytes(499) is None
        increment = manager.process_bytes(1)
        assert increment == 500
        assert manager.current_window_size == 900

    def test_default_policy_unblocks_empty_window(self) -> None:
        manager = h2.windows.WindowManager(10000)
        manager.window_consumed(10000)
        assert manager.process_bytes(1024) is None
        assert manager.process_bytes(1) == 1025
        assert manager.current_window_size == 1025

    def test_default_policy_uses_quarter_when_small(self) -> None:
        # For windows smaller than 4096 the unblock threshold is 1/4 of it.
        manager = h2.windows.WindowManager(1000)
        manager.window_consumed(1000)
        assert manager.process_bytes(250) is None
        assert manager.process_bytes(1) == 251

    def test_default_policy_is_capped_by_available_headroom(self) -> None:
        manager = h2.windows.WindowManager(1000)
        # Drain the window, then open it fully by hand, leaving no headroom.
        manager.window_consumed(1000)
        manager.window_opened(1000)
        # The policy fires (half window consumed) but cannot add anything,
        # and the consumed allowance is cleared.
        assert manager.process_bytes(500) is None
        assert manager.current_window_size == 1000
        assert manager.process_bytes(500) is None

    def test_no_bytes_processed_does_not_invoke_policy(self) -> None:
        class FailingPolicy:
            def window_increment(self, consumed_size, state):
                raise AssertionError("must not be called")

        manager = h2.windows.WindowManager(
            1000, window_update_policy=FailingPolicy,
        )
        assert manager.process_bytes(0) is None

    def test_policy_receives_consumed_size_and_window_state(self) -> None:
        seen: list = []

        class Policy:
            def window_increment(self, consumed_size, state):
                seen.append((consumed_size, state.stream_id,
                             state.bytes_processed,
                             state.current_window_size,
                             state.max_window_size))
                return None

        manager = h2.windows.WindowManager(100, Policy, stream_id=7)
        manager.window_consumed(30)
        manager.process_bytes(10)
        assert seen == [(10, 7, 10, 70, 100)]

    def test_policy_can_split_increment_across_calls(self) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([30, None]),
        )
        manager.window_consumed(100)
        assert manager.process_bytes(50) == 30
        assert manager.current_window_size == 30
        # 20 bytes stay banked; a no-update answer keeps them banked.
        assert manager.process_bytes(0) is None
        assert manager.current_window_size == 30
        # Newly consumed bytes are added to the banked remainder.
        assert manager.process_bytes(10) is None

    @pytest.mark.parametrize("bad_value", [-1, -100])
    def test_negative_increment_rejected(self, bad_value: int) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([bad_value]),
        )
        manager.window_consumed(10)
        with pytest.raises(h2.exceptions.InvalidWindowUpdateError):
            manager.process_bytes(10)
        # Nothing was applied.
        assert manager.current_window_size == 90

    def test_increment_larger_than_released_quota_rejected(self) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([11]),
        )
        manager.window_consumed(10)
        with pytest.raises(h2.exceptions.InvalidWindowUpdateError):
            manager.process_bytes(10)
        assert manager.current_window_size == 90

    def test_overflowing_increment_rejected(self) -> None:
        # A policy cannot grow the window past 2**31 - 1. Arrange a window
        # that is nearly full while the application has released enough quota
        # for the malicious suggestion, then have the policy suggest it.
        manager = h2.windows.WindowManager(LARGEST_FLOW_CONTROL_WINDOW)
        manager.current_window_size = LARGEST_FLOW_CONTROL_WINDOW - 1
        manager._bytes_processed = 2
        manager.policy = ScriptedPolicy([2])
        with pytest.raises(h2.exceptions.InvalidWindowUpdateError):
            manager.process_bytes(0)
        assert manager.current_window_size == LARGEST_FLOW_CONTROL_WINDOW - 1
        assert manager._bytes_processed == 2

    def test_non_integer_increment_rejected(self) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([1.5]),
        )
        manager.window_consumed(10)
        with pytest.raises(h2.exceptions.InvalidWindowUpdateError):
            manager.process_bytes(10)

    def test_bool_increment_rejected(self) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([True]),
        )
        manager.window_consumed(10)
        with pytest.raises(h2.exceptions.InvalidWindowUpdateError):
            manager.process_bytes(10)

    def test_factory_must_return_valid_policy(self) -> None:
        class NotAPolicy:
            pass

        with pytest.raises(TypeError):
            h2.windows.WindowManager(100, NotAPolicy)

    def test_zero_is_a_valid_no_frame_answer_that_clears_quota(self) -> None:
        manager = h2.windows.WindowManager(
            100, lambda: ScriptedPolicy([0]),
        )
        manager.window_consumed(10)
        assert manager.process_bytes(10) is None
        # The allowance was cleared even though no update was emitted.
        assert manager.current_window_size == 90


class TestConfiguration:
    def test_policy_must_be_callable(self) -> None:
        with pytest.raises(ValueError):
            h2.config.H2Configuration(window_update_policy=42)

    def test_none_is_accepted(self) -> None:
        config = h2.config.H2Configuration(window_update_policy=None)
        assert config.window_update_policy is None

    def test_policy_attribute_is_validated_on_set(self) -> None:
        config = h2.config.H2Configuration()
        with pytest.raises(ValueError):
            config.window_update_policy = "not callable"
        config.window_update_policy = GreedyPolicy
        assert config.window_update_policy is GreedyPolicy

    def test_setting_attribute_back_to_none_restores_default(self) -> None:
        config = h2.config.H2Configuration(window_update_policy=GreedyPolicy)
        config.window_update_policy = None
        connection = h2.connection.H2Connection(config=config)
        assert isinstance(
            connection._inbound_flow_control_window_manager.policy,
            h2.windows.DefaultWindowUpdatePolicy,
        )

    def test_factory_returning_invalid_object_fails_connection_setup(self) -> None:
        class NotAPolicy:
            pass

        config = h2.config.H2Configuration(
            client_side=False,
            window_update_policy=NotAPolicy,
        )
        with pytest.raises(TypeError):
            h2.connection.H2Connection(config=config)


class TestDoubleEndedPolicy:
    """Two-connection simulations with frame-by-frame WINDOW_UPDATE checks."""

    def consume(self, pair: Pair, size: int, stream_id: int = 1) -> dict:
        pair.server.acknowledge_received_data(size, stream_id)
        _, server_bytes = pair.drain()
        return window_updates(server_bytes)

    def test_greedy_policy_emits_update_per_consumed_batch(self) -> None:
        pair = Pair(server_policy=GreedyPolicy)
        pair.open_stream()

        # DATA receipt itself emits nothing: the policy only runs on
        # acknowledge_received_data.
        client_bytes, server_bytes = pair.send_client_data(b"\x00" * 100)
        assert window_updates(client_bytes) == {}
        assert window_updates(server_bytes) == {}

        assert self.consume(pair, 100) == {0: [100], 1: [100]}

        # Second, larger batch: exactly its size comes back, frame by frame.
        pair.send_client_data(b"\x00" * 250)
        assert self.consume(pair, 250) == {0: [250], 1: [250]}

        assert pair.server.inbound_flow_control_window == 65535
        assert pair.server.local_flow_control_window(1) == 65535

    def test_partial_consumption_banks_remainder(self) -> None:
        pair = Pair(server_policy=lambda: ThresholdPolicy(300))
        pair.open_stream()
        pair.send_client_data(b"\x00" * 100)

        # App consumes only 40 of the 100 received: under threshold.
        assert self.consume(pair, 40) == {}
        # Consume another 60: bank reaches 100, still under threshold.
        assert self.consume(pair, 60) == {}

        # 200 more bytes arrive and are consumed: bank hits 300 and fires.
        pair.send_client_data(b"\x00" * 200)
        assert self.consume(pair, 200) == {0: [300], 1: [300]}

    def test_padded_data_accounts_for_padding(self) -> None:
        pair = Pair(server_policy=GreedyPolicy)
        pair.open_stream()
        pair.send_client_data(b"\x00" * 10, pad_length=5)
        # Flow controlled length is data + 1 length byte + 5 padding.
        assert self.consume(pair, 16) == {0: [16], 1: [16]}

        # pad_length=0 still costs one flow controlled byte.
        pair.send_client_data(b"\x00" * 10, pad_length=0)
        assert self.consume(pair, 11) == {0: [11], 1: [11]}

    def test_connection_window_replenished_after_remote_reset(self) -> None:
        """
        After the remote peer has reset a stream, the application may still
        finish consuming the data that had been delivered. That consumption
        must replenish the connection window (exactly, once), but never the
        stream window.
        """
        pair = Pair(server_policy=GreedyPolicy)
        pair.open_stream()
        pair.send_client_data(b"\x00" * 1000)

        # Client aborts the stream with a RST_STREAM.
        pair.client.reset_stream(1)
        client_bytes, server_bytes = pair.drain()
        assert [f.stream_id for f in parse_frames(client_bytes)
                if isinstance(f, RstStreamFrame)] == [1]
        assert window_updates(server_bytes) == {}

        assert pair.server.inbound_flow_control_window == 65535 - 1000

        # Application finishes consuming the buffered data: connection update
        # only, never a stream update.
        assert self.consume(pair, 1000) == {0: [1000]}
        assert pair.server.inbound_flow_control_window == 65535

    def test_inflight_data_after_local_reset_auto_acks_connection(self) -> None:
        """
        A DATA frame that arrives after the server locally reset the stream is
        a stream error. The DATA still consumed the connection window, so the
        server auto-replenishes the connection (under a greedy policy) and
        sends a RST_STREAM, but never a stream WINDOW_UPDATE.
        """
        pair = Pair(server_policy=GreedyPolicy)
        pair.open_stream()
        pair.server.reset_stream(1)
        pair.drain()

        late_data = DataFrame(1)
        late_data.data = b"\x00" * 100
        pair.server.receive_data(late_data.serialize())
        server_bytes = pair.server.data_to_send()

        assert window_updates(server_bytes) == {0: [100]}
        assert [f.stream_id for f in parse_frames(server_bytes)
                if isinstance(f, RstStreamFrame)] == [1]

    def test_interleaved_batch_consumption_and_reset_accounting(self) -> None:
        """
        Frame-by-frame accounting while two streams are consumed in batches and
        one is reset mid-flight. The mere fact that all data could eventually
        be delivered is not enough: every WINDOW_UPDATE is checked against the
        exact bytes consumed and the window that emitted it.
        """
        pair = Pair(server_policy=lambda: ThresholdPolicy(500))
        pair.open_stream(1)
        pair.open_stream(3)

        # Stream 1 receives 300 bytes; consuming all of them banks 300 and
        # emits nothing.
        pair.send_client_data(b"\x00" * 300, stream_id=1)
        assert self.consume(pair, 300, 1) == {}

        # Stream 3 receives 200 bytes. The connection bank now holds 500,
        # which fires one connection update; stream 3's own bank (200) stays
        # quiet.
        pair.send_client_data(b"\x00" * 200, stream_id=3)
        assert self.consume(pair, 200, 3) == {0: [500]}

        # Stream 1 receives another 200 bytes: its bank (500) fires a stream
        # update; the connection bank (200) stays quiet.
        pair.send_client_data(b"\x00" * 200, stream_id=1)
        assert self.consume(pair, 200, 1) == {1: [500]}

        # The client resets stream 1. The application then finishes consuming
        # the 200 bytes it had already buffered: no stream frame is possible
        # for a dead stream, and the connection bank (400) is below the 500
        # threshold, so nothing goes out yet.
        pair.client.reset_stream(1)
        pair.drain()
        assert self.consume(pair, 200, 1) == {}

        # Stream 3 keeps working: 100 more consumed bytes push the connection
        # bank over 500 (one connection frame only, for 500); stream 3's own
        # bank (300) is still under threshold.
        pair.send_client_data(b"\x00" * 100, stream_id=3)
        assert self.consume(pair, 100, 3) == {0: [500]}

        # The remaining 200 bytes bring stream 3's bank to 500 while the
        # connection bank only holds 200, so only the stream frame is emitted.
        pair.send_client_data(b"\x00" * 200, stream_id=3)
        assert self.consume(pair, 200, 3) == {3: [500]}

        # The surviving stream's window is fully replenished (200 + 300).
        assert pair.server.local_flow_control_window(3) == 65535

    def test_initial_window_size_change_feeds_policy_correctly(self) -> None:
        """
        A SETTINGS_INITIAL_WINDOW_SIZE change alters both the maximum and the
        current stream window; subsequent consumption works and the stream
        update restores the window to the new initial value.
        """
        pair = Pair(
            server_policy=lambda: ThresholdPolicy(100),
            initial_window_size=1000,
        )
        pair.open_stream()
        assert pair.server.local_flow_control_window(1) == 1000

        pair.send_client_data(b"\x00" * 100)
        assert self.consume(pair, 100) == {0: [100], 1: [100]}
        assert pair.server.local_flow_control_window(1) == 1000

    def test_policy_state_is_isolated_between_scopes(self) -> None:
        policies: list = []

        def factory():
            policy = GreedyPolicy()
            policies.append(policy)
            return policy

        pair = Pair(server_policy=factory)
        pair.open_stream(1)
        pair.open_stream(3)
        pair.send_client_data(b"\x00" * 10, stream_id=1)
        pair.send_client_data(b"\x00" * 20, stream_id=3)
        pair.server.acknowledge_received_data(10, 1)
        pair.server.acknowledge_received_data(20, 3)
        pair.drain()

        by_stream = {
            state.stream_id: policy
            for policy in policies
            for _, state in policy.calls
        }
        assert set(by_stream) == {0, 1, 3}
        # The connection policy saw both batches; each stream policy only its
        # own bytes.
        assert [consumed for consumed, _ in by_stream[0].calls] == [10, 20]
        assert [consumed for consumed, _ in by_stream[1].calls] == [10]
        assert [consumed for consumed, _ in by_stream[3].calls] == [20]

    def test_separate_connections_get_separate_policy_instances(self) -> None:
        created = []

        def factory():
            policy = GreedyPolicy()
            created.append(policy)
            return policy

        configs = [
            h2.config.H2Configuration(client_side=False,
                                      window_update_policy=factory)
            for _ in range(2)
        ]
        connections = [h2.connection.H2Connection(config=c) for c in configs]
        assert created[0] is not created[1]
        assert connections[0] is not connections[1]

    def test_default_policy_timing_preserved_double_ended(self) -> None:
        pair = Pair()
        pair.open_stream()

        # Small consumption: no updates at all.
        pair.send_client_data(b"\x00" * 1000)
        assert self.consume(pair, 1000) == {}

        # Drain the window completely, then ack enough to pass the half-window
        # threshold. The 1000 earlier bytes remained banked and are included.
        pair.send_client_data(b"\x00" * (65535 - 1000))
        updates = self.consume(pair, 33000)
        assert updates[0] == [34000]
        assert updates[1] == [34000]
