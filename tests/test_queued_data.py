"""
test_queued_data
~~~~~~~~~~~~~~~~

Tests for the queued DATA sending API: ``H2Connection.queue_data``.
"""
from __future__ import annotations

import pytest

import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings
import h2.stream


class TestQueuedData:
    """
    Tests of the queued data sending machinery.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]

    DEFAULT_FLOW_WINDOW = 65535

    def _connection_with_stream(self) -> h2.connection.H2Connection:
        """
        Return a client connection with stream 1 open and the outbound
        buffer cleared of any setup bytes.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()
        c.send_headers(1, self.example_request_headers)
        c.clear_outbound_data_buffer()
        return c

    def _expected_data(self, frame_factory, chunks, stream_id=1) -> bytes:
        """
        Build the expected serialized output for a sequence of DATA frames.
        Each chunk is either ``bytes`` or a ``(bytes, flags)`` tuple.
        """
        expected = b""
        for chunk in chunks:
            if isinstance(chunk, tuple):
                data, flags = chunk
            else:
                data, flags = chunk, None
            expected += frame_factory.build_data_frame(
                data, flags=flags, stream_id=stream_id,
            ).serialize()
        return expected

    def test_queue_data_sends_immediately_when_window_allows(
        self, frame_factory,
    ) -> None:
        """
        When the flow control windows allow it, queued data is emitted
        immediately as DATA frames.
        """
        c = self._connection_with_stream()

        accepted = c.queue_data(1, b"some data")

        assert accepted == len(b"some data")
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"some data"],
        )
        assert c.local_flow_control_window(1) == (
            self.DEFAULT_FLOW_WINDOW - len(b"some data")
        )

    def test_queue_data_splits_on_max_frame_size(self, frame_factory) -> None:
        """
        Queued data larger than the maximum frame size is split into
        multiple DATA frames.
        """
        c = self._connection_with_stream()
        c.max_outbound_frame_size = 5

        accepted = c.queue_data(1, b"0123456789")

        assert accepted == 10
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"01234", b"56789"],
        )

    def test_queue_data_waits_for_connection_window_update(
        self, frame_factory,
    ) -> None:
        """
        Data that does not fit in the connection flow control window is
        buffered and emitted when a connection-level WINDOW_UPDATE arrives.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 5

        accepted = c.queue_data(1, b"0123456789")

        assert accepted == 10
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"01234"],
        )

        events = c.receive_data(
            frame_factory.build_window_update_frame(0, 5).serialize(),
        )

        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"56789"],
        )
        assert any(
            isinstance(e, h2.events.WindowUpdated) for e in events
        )

    def test_queue_data_waits_for_stream_window_update(self, frame_factory) -> None:
        """
        Data that does not fit in the stream flow control window is
        buffered and emitted when a stream-level WINDOW_UPDATE arrives.
        """
        c = self._connection_with_stream()
        c._get_stream_by_id(1).outbound_flow_control_window = 5

        accepted = c.queue_data(1, b"0123456789")

        assert accepted == 10
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"01234"],
        )

        c.receive_data(
            frame_factory.build_window_update_frame(1, 5).serialize(),
        )

        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"56789"],
        )

    def test_two_streams_share_connection_window(self, frame_factory) -> None:
        """
        Two streams with queued data share the connection flow control
        window in a round-robin fashion.
        """
        c = self._connection_with_stream()
        c.send_headers(3, self.example_request_headers)
        c.clear_outbound_data_buffer()
        c.max_outbound_frame_size = 5
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"a" * 15) == 15
        assert c.queue_data(3, b"b" * 15) == 15
        assert c.data_to_send() == b""

        # Each stream gets one frame per round until the connection window
        # is exhausted.
        expected = (
            self._expected_data(frame_factory, [b"a" * 5], stream_id=1) +
            self._expected_data(frame_factory, [b"b" * 5], stream_id=3)
        )
        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == expected

        # Further connection-level WINDOW_UPDATEs let both streams
        # continue to make progress.
        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == expected

        # One more window update drains both queues.
        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == expected
        assert not c._queued_data

    def test_negative_stream_window_recovers_via_settings(
        self, frame_factory,
    ) -> None:
        """
        A SETTINGS reduction of INITIAL_WINDOW_SIZE can push a stream
        window negative: queued data waits until a later SETTINGS change
        makes the window positive again.
        """
        c = self._connection_with_stream()

        # Send 100 bytes, then shrink the initial window to 0: the stream
        # window becomes negative.
        assert c.queue_data(1, b"x" * 100) == 100
        c.clear_outbound_data_buffer()
        settings_frame = frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0},
        )
        c.receive_data(settings_frame.serialize())
        c.clear_outbound_data_buffer()
        assert c._get_stream_by_id(1).outbound_flow_control_window == -100

        # This data is accepted but cannot be emitted yet.
        assert c.queue_data(1, b"y" * 50) == 50
        assert c.data_to_send() == b""

        # Restoring the initial window makes the stream window positive
        # again, and the queued data is emitted without any WINDOW_UPDATE.
        settings_frame = frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 65535},
        )
        c.receive_data(settings_frame.serialize())

        sent = c.data_to_send()
        assert sent.endswith(
            self._expected_data(frame_factory, [b"y" * 50]),
        )

    def test_max_frame_size_change_applies_to_queued_data(
        self, frame_factory,
    ) -> None:
        """
        A SETTINGS change to MAX_FRAME_SIZE changes the size of the DATA
        frames emitted for queued data.
        """
        c = self._connection_with_stream()

        assert c.queue_data(1, b"z" * 40000) == 40000
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"z" * 16384, b"z" * 16384, b"z" * 7232],
        )

        # The peer raises the maximum frame size: subsequent queued data
        # is emitted in larger frames.
        settings_frame = frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: 32768},
        )
        c.receive_data(settings_frame.serialize())

        # Replenish the flow control windows consumed above.
        c.receive_data(
            frame_factory.build_window_update_frame(0, 40000).serialize() +
            frame_factory.build_window_update_frame(1, 40000).serialize(),
        )
        c.clear_outbound_data_buffer()

        assert c.queue_data(1, b"z" * 40000) == 40000
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"z" * 32768, b"z" * 7232],
        )

    def test_end_stream_only_on_final_chunk(self, frame_factory) -> None:
        """
        When queued data is emitted in several frames, END_STREAM appears
        only on the frame carrying the last byte.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 5

        accepted = c.queue_data(1, b"0123456789", end_stream=True)

        assert accepted == 10
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"01234"],
        )

        c.receive_data(
            frame_factory.build_window_update_frame(0, 5).serialize(),
        )
        assert c.data_to_send() == self._expected_data(
            frame_factory, [(b"56789", ["END_STREAM"])],
        )

    def test_empty_final_frame_with_end_stream(self, frame_factory) -> None:
        """
        Queueing zero bytes with end_stream emits an empty DATA frame with
        END_STREAM, even when the flow control window is exhausted.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, b"", end_stream=True)

        assert accepted == 0
        assert c.data_to_send() == self._expected_data(
            frame_factory, [(b"", ["END_STREAM"])],
        )

    def test_empty_end_stream_merges_with_queued_data(self, frame_factory) -> None:
        """
        An empty end_stream queued behind blocked data rides on the final
        data frame once the blocked data can be sent.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"data") == 4
        assert c.queue_data(1, b"", end_stream=True) == 0
        assert c.data_to_send() == b""

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == self._expected_data(
            frame_factory, [(b"data", ["END_STREAM"])],
        )

    def test_local_reset_discards_queued_data(self, frame_factory) -> None:
        """
        Resetting a stream discards its queued data: later window updates
        emit nothing for it.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10
        assert c._queued_data_size == 10

        c.reset_stream(1)
        c.clear_outbound_data_buffer()
        assert c._queued_data_size == 0

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == b""

    def test_received_reset_discards_queued_data(self, frame_factory) -> None:
        """
        A RST_STREAM received from the peer discards queued data for that
        stream.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10

        rst = frame_factory.build_rst_stream_frame(1)
        c.receive_data(rst.serialize())
        assert c._queued_data_size == 0

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c.data_to_send() == b""

    def test_close_connection_discards_queued_data(self, frame_factory) -> None:
        """
        Sending a GOAWAY frame discards all queued data.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10

        c.close_connection()
        assert c._queued_data_size == 0
        assert not c._queued_data

    def test_received_goaway_discards_queued_data(self, frame_factory) -> None:
        """
        A GOAWAY frame received from the peer discards all queued data.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10

        goaway = frame_factory.build_goaway_frame(last_stream_id=0)
        c.receive_data(goaway.serialize())
        assert c._queued_data_size == 0
        assert not c._queued_data

    def test_buffer_limit_reports_accepted_bytes(self, frame_factory) -> None:
        """
        The queue never buffers more than the configured limit: the caller
        is told how many bytes were accepted.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0
        c.max_buffered_data_bytes = 10

        assert c.queue_data(1, b"0123456789" * 3) == 10
        assert c._queued_data_size == 10

        # The buffer is full: nothing more is accepted.
        assert c.queue_data(1, b"more data") == 0
        assert c._queued_data_size == 10

        # Once the window opens the buffer drains and more data fits.
        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert c._queued_data_size == 0
        assert c.queue_data(1, b"more data") == 9

    def test_buffer_limit_configurable_per_call(self, frame_factory) -> None:
        """
        The buffer limit can be overridden on a per-call basis.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789", max_buffered_bytes=5) == 5
        assert c._queued_data_size == 5

    def test_partial_acceptance_does_not_set_end_stream(self, frame_factory) -> None:
        """
        If end_stream is requested but not all data is accepted, the
        stream is not ended: the caller re-queues the remainder.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0
        c.max_buffered_data_bytes = 5

        # Only half the data fits in the buffer: end_stream is not
        # registered, and the caller must re-queue the remainder later.
        assert c.queue_data(1, b"0123456789", end_stream=True) == 5
        assert c.queue_data(1, b"56789", end_stream=True) == 0

        c.receive_data(
            frame_factory.build_window_update_frame(0, 5).serialize(),
        )
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"01234"],
        )

        # With buffer space available again, the remainder is accepted and
        # END_STREAM is emitted on the final chunk.
        assert c.queue_data(1, b"56789", end_stream=True) == 5
        c.receive_data(
            frame_factory.build_window_update_frame(0, 5).serialize(),
        )
        assert c.data_to_send() == self._expected_data(
            frame_factory, [(b"56789", ["END_STREAM"])],
        )

    def test_queue_data_on_unknown_stream_fails(self) -> None:
        """
        Queueing data on a stream that does not exist raises
        NoSuchStreamError.
        """
        c = self._connection_with_stream()

        with pytest.raises(h2.exceptions.NoSuchStreamError):
            c.queue_data(3, b"nope")

    def test_queue_data_on_ended_stream_fails(self, frame_factory) -> None:
        """
        Queueing data on a stream we have already ended raises
        ProtocolError.
        """
        c = self._connection_with_stream()
        c.end_stream(1)

        with pytest.raises(h2.exceptions.ProtocolError):
            c.queue_data(1, b"nope")

    def test_queue_data_after_end_stream_queued_fails(self, frame_factory) -> None:
        """
        Once end_stream has been queued, no more data can be queued on the
        stream.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"data", end_stream=True) == 4
        with pytest.raises(h2.exceptions.ProtocolError):
            c.queue_data(1, b"more")

    def test_queue_data_rejects_negative_buffer_limit(self, frame_factory) -> None:
        """
        A negative max_buffered_bytes value is rejected.
        """
        c = self._connection_with_stream()

        with pytest.raises(ValueError):
            c.queue_data(1, b"data", max_buffered_bytes=-1)

    def test_queue_data_accepts_memoryview(self, frame_factory) -> None:
        """
        queue_data accepts any bytes-like object, such as memoryview.
        """
        c = self._connection_with_stream()

        accepted = c.queue_data(1, memoryview(b"some data"))

        assert accepted == len(b"some data")
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"some data"],
        )

    def test_drain_discards_queue_for_closed_stream(self, frame_factory) -> None:
        """
        If a stream closes while data is still queued, the queued data is
        discarded rather than emitted.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10

        # Forcibly close the stream, bypassing the usual reset handling.
        c._get_stream_by_id(1).state_machine.state = (
            h2.stream.StreamState.CLOSED
        )

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert not c._queued_data
        assert c.data_to_send() == b""

    def test_drain_discards_queue_for_removed_stream(self, frame_factory) -> None:
        """
        If a stream is cleaned up while data is still queued, the queued
        data is discarded rather than emitted.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10

        del c.streams[1]

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert not c._queued_data
        assert c.data_to_send() == b""

    def test_drain_discards_empty_entry_without_end_stream(
        self, frame_factory,
    ) -> None:
        """
        A queue entry holding no data and no END_STREAM is cleaned up
        without emitting anything.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        c._queued_data[1] = h2.connection._QueuedData()

        c.receive_data(
            frame_factory.build_window_update_frame(0, 10).serialize(),
        )
        assert not c._queued_data
        assert c.data_to_send() == b""

    def test_send_data_rejects_streams_with_queued_data(self, frame_factory) -> None:
        """
        send_data cannot be mixed with queue_data on a stream that has
        queued data pending.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10
        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_data(1, b"other")

    def test_end_stream_rejects_streams_with_queued_data(
        self, frame_factory,
    ) -> None:
        """
        end_stream cannot be used on a stream that has queued data
        pending.
        """
        c = self._connection_with_stream()
        c.outbound_flow_control_window = 0

        assert c.queue_data(1, b"0123456789") == 10
        with pytest.raises(h2.exceptions.ProtocolError):
            c.end_stream(1)

    def test_send_data_unaffected_when_queue_drained(self, frame_factory) -> None:
        """
        Once the queue for a stream has fully drained, send_data works on
        it as before.
        """
        c = self._connection_with_stream()

        assert c.queue_data(1, b"queued") == 6
        c.clear_outbound_data_buffer()

        c.send_data(1, b"direct")
        assert c.data_to_send() == self._expected_data(
            frame_factory, [b"direct"],
        )
