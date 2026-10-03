"""
h2/windows
~~~~~~~~~~

Defines tools for managing HTTP/2 flow control windows.

The objects defined in this module are used to automatically manage HTTP/2
flow control windows. Specifically, they keep track of what the size of the
window is, how much data has been consumed from that window, and how much data
the user has already used. It then runs a window update policy that decides
when the space should be handed back to the remote peer. The default policy
implements the historical algorithm, trying to ensure that it does not emit
too many WINDOW_UPDATE frames, but users may supply their own policy to base
that decision on their own notion of backpressure, such as a memory watermark.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable

from .exceptions import FlowControlError, InvalidWindowUpdateError

# The largest acceptable value for a HTTP/2 flow control window.
LARGEST_FLOW_CONTROL_WINDOW = 2**31 - 1


@dataclass(frozen=True)
class WindowState:
    """
    A snapshot of the state of a single flow control window that is passed to
    a :class:`WindowUpdatePolicy` whenever the application has consumed data.

    :param stream_id: The stream ID this window belongs to, or ``0`` for the
        connection level window.
    :param bytes_processed: The number of flow controlled bytes that have been
        consumed since the window was last replenished (and therefore the
        maximum increment it is legal to hand back right now).
    :param current_window_size: The current size of the inbound flow control
        window. This shrinks as DATA frames arrive and grows when a
        WINDOW_UPDATE is emitted.
    :param max_window_size: The largest size this window has ever had, i.e.
        the "full" size the window is replenished towards under the default
        policy.
    """

    stream_id: int
    bytes_processed: int
    current_window_size: int
    max_window_size: int


@runtime_checkable
class WindowUpdatePolicy(Protocol):
    """
    The interface implemented by objects that decide when an inbound flow
    control window should be replenished.

    A policy receives the number of bytes the application has just consumed
    and a snapshot of the current window state. It returns the number of bytes
    the window should be incremented by, or ``None`` (equivalently ``0``) to
    hand no space back on this call.

    The returned increment must satisfy three constraints:

    * it must not be negative;
    * it must not be greater than ``bytes_processed``, i.e. a policy cannot
      hand back space the application has not released;
    * it must not grow the window beyond ``2**31 - 1`` bytes.

    Returning an illegal value raises :class:`InvalidWindowUpdateError
    <h2.exceptions.InvalidWindowUpdateError>`.

    Policies may keep state: h2 instantiates a fresh policy object for each
    connection-level window and for each stream-level window, so state is
    never shared between connections or streams.
    """

    def window_increment(
        self,
        consumed_size: int,
        state: WindowState,
    ) -> int | None:
        """
        Decide how many bytes to hand back to the remote peer.

        :param consumed_size: The number of flow controlled bytes the
            application has just consumed.
        :param state: A snapshot of the window state.
        :returns: The number of bytes to increment the window by, or ``None``
            to leave the window untouched.
        """
        ...


class DefaultWindowUpdatePolicy:
    """
    The window update policy that h2 has always used.

    Its algorithm can be described like this.

    1. If no bytes have been processed, no WINDOW_UPDATE is emitted.
    2. If there is no space in the flow control window, and we have
       processed at least 1024 bytes (or 1/4 of the window, if the window
       is smaller), we will emit a window update frame. This is to avoid
       the risk of blocking a stream altogether.
    3. If there is space in the flow control window, and we have processed
       at least 1/2 of the window worth of bytes, we will emit a window
       update frame. This is to minimise the number of window update frames
       we have to emit.

    In a healthy system with large flow control windows, this will
    irregularly emit WINDOW_UPDATE frames. This prevents us starving the
    connection by emitting eleventy bajillion WINDOW_UPDATE frames,
    especially in situations where the remote peer is sending a lot of very
    small DATA frames.
    """

    def window_increment(
        self,
        consumed_size: int,
        state: WindowState,
    ) -> int | None:
        if not state.bytes_processed:
            return None

        # TODO: Can the window be smaller than 1024 bytes? If not, we can
        # streamline this algorithm.
        window_empty = state.current_window_size == 0
        unblock_threshold = min(1024, state.max_window_size // 4)
        half_window = state.max_window_size // 2
        if (window_empty and state.bytes_processed > unblock_threshold) or (
            state.bytes_processed >= half_window
        ):
            # Never hand back more space than the window is missing: the user
            # may already have opened the window manually.
            headroom = state.max_window_size - state.current_window_size
            return min(state.bytes_processed, headroom)
        return None


class WindowManager:
    """
    A basic HTTP/2 window manager.

    :param max_window_size: The maximum size of the flow control window.
    :type max_window_size: ``int``
    :param window_update_policy: A zero-argument callable used to construct
        the policy that decides when this window is replenished. It is called
        once per :class:`WindowManager`, which guarantees that connection and
        stream windows (and different connections) get independent policy
        state. Defaults to :class:`DefaultWindowUpdatePolicy`.
    :type window_update_policy: callable
    :param stream_id: The stream ID this manager belongs to, or ``0`` for the
        connection level window. Only used to inform the policy.
    :type stream_id: ``int``
    """

    def __init__(
        self,
        max_window_size: int,
        window_update_policy: Callable[[], WindowUpdatePolicy] | None = None,
        stream_id: int = 0,
    ) -> None:
        assert max_window_size <= LARGEST_FLOW_CONTROL_WINDOW
        self.max_window_size = max_window_size
        self.current_window_size = max_window_size
        self._bytes_processed = 0

        if window_update_policy is None:
            window_update_policy = DefaultWindowUpdatePolicy
        self.policy = window_update_policy()
        if not isinstance(self.policy, WindowUpdatePolicy):
            msg = (
                "window_update_policy must return an object providing a "
                "window_increment(consumed_size, state) method"
            )
            raise TypeError(msg)

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
        # Opening the window by hand does not acknowledge any received data:
        # keep the processed-byte bank untouched so the policy remains able
        # to hand exactly that quota back later.
        self.current_window_size += size

        if self.current_window_size > LARGEST_FLOW_CONTROL_WINDOW:
            msg = f"Flow control window mustn't exceed {LARGEST_FLOW_CONTROL_WINDOW}"
            raise FlowControlError(msg)

        self.max_window_size = max(self.current_window_size, self.max_window_size)

    def process_bytes(self, size: int) -> int | None:
        """
        The application has informed us that it has processed a certain number
        of bytes. The configured window update policy is consulted and may
        return the number of bytes by which the window should be incremented.

        :param size: The number of flow controlled bytes that the application
        has processed.
        :type size: ``int``
        :returns: The number of bytes to increment the flow control window by,
            or ``None``.
        :rtype: ``int`` or ``None``
        """
        self._bytes_processed += size
        return self._maybe_update_window(size)

    def _maybe_update_window(self, consumed_size: int) -> int | None:
        """
        Consult the configured policy and validate its suggestion.

        Returning ``None`` means the policy does not want to replenish the
        window: the consumed bytes remain "in the bank" and are offered to the
        policy again on the next call. Returning an integer (including ``0``)
        means the policy has accounted for the bytes it was told about, so the
        bank is cleared even when no increment is actually emitted.
        """
        if not self._bytes_processed:
            return None

        state = WindowState(
            stream_id=self.stream_id,
            bytes_processed=self._bytes_processed,
            current_window_size=self.current_window_size,
            max_window_size=self.max_window_size,
        )
        increment = self.policy.window_increment(consumed_size, state)

        if increment is None:
            return None
        if not isinstance(increment, int) or isinstance(increment, bool):
            msg = "Window update policy must return an int or None"
            raise InvalidWindowUpdateError(msg)
        if increment < 0:
            msg = "Window update policy must not return a negative increment"
            raise InvalidWindowUpdateError(msg)
        if increment > self._bytes_processed:
            msg = (
                f"Window update policy suggested an increment of {increment} "
                f"bytes but only {self._bytes_processed} bytes have been "
                "released"
            )
            raise InvalidWindowUpdateError(msg)
        if self.current_window_size + increment > LARGEST_FLOW_CONTROL_WINDOW:
            msg = (
                f"Window update policy suggested an increment of {increment} "
                "bytes but the flow control window must not exceed "
                f"{LARGEST_FLOW_CONTROL_WINDOW}"
            )
            raise InvalidWindowUpdateError(msg)

        self.current_window_size += increment
        self._bytes_processed = 0
        return increment if increment else None
