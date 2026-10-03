"""
h2/windows
~~~~~~~~~~

Defines tools for managing HTTP/2 flow control windows.

The objects defined in this module are used to automatically manage HTTP/2
flow control windows. Specifically, they keep track of what the size of the
window is, how much data has been consumed from that window, and how much data
the user has already used. It then implements a basic algorithm that attempts
to manage the flow control window without user input, trying to ensure that it
does not emit too many WINDOW_UPDATE frames.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .exceptions import FlowControlError

# The largest acceptable value for a HTTP/2 flow control window.
LARGEST_FLOW_CONTROL_WINDOW = 2**31 - 1


@dataclass(frozen=True, slots=True)
class WindowUpdateContext:
    """
    Information supplied to a window update policy.

    :param stream_id: The stream ID, or ``0`` for the connection window.
    :param consumed_bytes: Flow-controlled bytes consumed but not yet returned
        to the peer.
    :param current_window_size: The current size of the receive window.
    :param max_window_size: The largest window size currently configured for
        this manager.
    :param available_window_size: The number of bytes that can be added before
        reaching ``max_window_size``.
    :param source_stream_id: For connection policies, the stream whose data was
        consumed, when it is known.
    """

    stream_id: int
    consumed_bytes: int
    current_window_size: int
    max_window_size: int
    available_window_size: int
    source_stream_id: int | None = None


@runtime_checkable
class WindowUpdatePolicy(Protocol):
    """
    A callable policy for deciding when receive windows are replenished.
    """

    def __call__(self, context: WindowUpdateContext) -> int | None:
        """
        Return the number of bytes to add to the receive window, or ``None``
        (or ``0``) if no ``WINDOW_UPDATE`` frame should be sent.
        """
        ...


class WindowUpdatePolicyFactory(Protocol):
    """
    A factory used to provide isolated policy state for a connection.
    """

    def __call__(self) -> WindowUpdatePolicy:
        """
        Create a window update policy.
        """
        ...


class DefaultWindowUpdatePolicy:
    """
    The automatic window update strategy historically used by h2.
    """

    def __call__(self, context: WindowUpdateContext) -> int | None:
        if self.should_update(context):
            return min(context.consumed_bytes, context.available_window_size)
        return None

    @staticmethod
    def should_update(context: WindowUpdateContext) -> bool:
        return bool(context.consumed_bytes) and (
            ((context.current_window_size == 0) and (
                context.consumed_bytes > min(1024, context.max_window_size // 4))) or
            context.consumed_bytes >= context.max_window_size // 2
        )


class WindowManager:
    """
    A basic HTTP/2 window manager.

    :param max_window_size: The maximum size of the flow control window.
    :type max_window_size: ``int``
    """

    def __init__(
        self,
        max_window_size: int,
        policy: WindowUpdatePolicy | None = None,
        stream_id: int = 0,
    ) -> None:
        assert max_window_size <= LARGEST_FLOW_CONTROL_WINDOW
        self.max_window_size = max_window_size
        self.current_window_size = max_window_size
        self._bytes_processed = 0
        self._window_update_policy = (
            policy if policy is not None else DefaultWindowUpdatePolicy()
        )
        self.stream_id = stream_id

    def window_consumed(self, size: int) -> None:
        """
        We have received a certain number of bytes from the remote peer. This
        necessarily shrinks the flow control window!

        :param size: The number of flow controlled bytes we received from the
            remote peer.
        :type size: ``int``
        :returns: Nothing.
        :rtype: ``None``
        """
        self.current_window_size -= size
        if self.current_window_size < 0:
            msg = "Flow control window shrunk below 0"
            raise FlowControlError(msg)

    def window_opened(self, size: int) -> None:
        """
        The flow control window has been incremented, either because of manual
        flow control management or because of the user changing the flow
        control settings. This can have the effect of increasing what we
        consider to be the "maximum" flow control window size.

        This does not increase our view of how many bytes have been processed,
        only of how much space is in the window.

        :param size: The increment to the flow control window we received.
        :type size: ``int``
        :returns: Nothing
        :rtype: ``None``
        """
        self.current_window_size += size

        if self.current_window_size > LARGEST_FLOW_CONTROL_WINDOW:
            msg = f"Flow control window mustn't exceed {LARGEST_FLOW_CONTROL_WINDOW}"
            raise FlowControlError(msg)

        self.max_window_size = max(self.current_window_size, self.max_window_size)

    def window_changed(self, delta: int) -> None:
        """
        Update a receive window in response to changed settings without
        consuming bytes or emitting a frame.
        """
        self.max_window_size += delta
        self.current_window_size += delta

        if self.current_window_size > LARGEST_FLOW_CONTROL_WINDOW:
            msg = f"Flow control window mustn't exceed {LARGEST_FLOW_CONTROL_WINDOW}"
            raise FlowControlError(msg)

    def process_bytes(self, size: int, source_stream_id: int | None = None) -> int | None:
        """
        The application has informed us that it has processed a certain number
        of bytes. This may cause us to want to emit a window update frame. If
        we do want to emit a window update frame, this method will return the
        number of bytes that we should increment the window by.

        :param size: The number of flow controlled bytes that the application
            has processed.
        :type size: ``int``
        :param source_stream_id: For a connection manager, the stream for
            which the application consumed the bytes.
        :type source_stream_id: ``int`` or ``None``
        :returns: The number of bytes to increment the flow control window by,
            or ``None``.
        :rtype: ``int`` or ``None``
        """
        if not isinstance(size, int) or isinstance(size, bool):
            msg = "Processed byte count must be an integer"
            raise TypeError(msg)
        if size < 0:
            msg = "Cannot process a negative number of bytes"
            raise ValueError(msg)

        consumed_bytes = self._bytes_processed + size
        available_window_size = self.max_window_size - self.current_window_size
        context = WindowUpdateContext(
            stream_id=self.stream_id,
            consumed_bytes=consumed_bytes,
            current_window_size=self.current_window_size,
            max_window_size=self.max_window_size,
            available_window_size=available_window_size,
            source_stream_id=source_stream_id,
        )
        increment = self._window_update_policy(context)
        if increment is None:
            increment = 0

        if not isinstance(increment, int) or isinstance(increment, bool):
            msg = "Window update policy must return an integer or None"
            raise TypeError(msg)
        if increment < 0:
            msg = "Window update policy returned a negative increment"
            raise ValueError(msg)
        if increment > consumed_bytes:
            msg = "Window update increment cannot exceed consumed bytes"
            raise ValueError(msg)
        if increment > LARGEST_FLOW_CONTROL_WINDOW:
            msg = "Window update increment exceeds the available receive window"
            raise OverflowError(msg)
        if increment > available_window_size:
            msg = "Window update increment cannot exceed the available window"
            raise OverflowError(msg)

        self._bytes_processed = consumed_bytes - increment
        self.current_window_size += increment
        return increment or None
