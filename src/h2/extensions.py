"""
h2/extensions
~~~~~~~~~~~~~

An interface for handling HTTP/2 extension frames.

HTTP/2 (RFC 9113 § 5.5) allows peers to send frames whose type is not
defined by the core specification. By default h2 ignores such frames, as
required by the RFC. Applications that deliberately speak an extension
protocol can instead register a handler for a specific frame type on a
specific connection, without modifying h2's core frame dispatch table.

Handlers are registered per connection via
:meth:`H2Connection.register_extension_handler
<h2.connection.H2Connection.register_extension_handler>`. The core
performs all protocol-level validation (frame length, CONTINUATION
continuity, connection state, and the handler's declared constraints)
*before* invoking the handler, and hands the handler an immutable payload.
"""
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from .connection import H2Connection
    from .events import Event


class StreamAssociation(Enum):
    """
    Declares whether an extension frame is meaningful at the connection
    level, at the stream level, or either.

    The core enforces this declaration before invoking the handler: a
    connection-level frame received with a nonzero stream ID (or a
    stream-level frame received on stream 0) is a connection error of type
    PROTOCOL_ERROR, and the handler is never called.
    """

    #: The frame applies to the whole connection: its stream ID must be 0.
    CONNECTION = 0

    #: The frame applies to a single stream: its stream ID must be nonzero.
    STREAM = 1

    #: The frame may appear with any stream ID.
    EITHER = 2


class ExtensionFrameHandler:
    """
    Base class for extension frame handlers.

    Subclasses must override :meth:`handle_frame` and should override the
    :attr:`stream_association` and :attr:`max_payload_size` class attributes
    to declare the constraints of their extension. An instance of the
    subclass is registered for a single frame type on a single connection;
    the registration has no effect on any other connection.
    """

    #: Whether frames of this type are connection-level, stream-level, or
    #: either. Enforced by the core before the handler is invoked.
    stream_association = StreamAssociation.EITHER

    #: The largest payload, in bytes, the handler is prepared to accept.
    #: Frames larger than this are rejected by the core with a
    #: :class:`FrameTooLargeError <h2.exceptions.FrameTooLargeError>` before
    #: the handler is invoked. This limit applies in addition to the
    #: connection-wide maximum frame size.
    max_payload_size: int = 2**14

    def handle_frame(
        self,
        connection: H2Connection,
        stream_id: int,
        flags: int,
        payload: bytes,
    ) -> Iterable[Event] | None:
        """
        Process a single extension frame.

        This method is only ever called after the core has validated the
        frame, and it is never called with a mutable view into h2's
        internal buffers: ``payload`` is an immutable ``bytes`` object.

        :param connection: The connection the frame was received on.
        :param stream_id: The stream ID the frame was received on.
        :param flags: The frame's raw flag byte, as an integer.
        :param payload: The frame payload as immutable ``bytes``.
        :returns: An iterable of events to emit to the application, or
            ``None``.
        :raises h2.exceptions.ProtocolError: If the frame violates the
            extension's protocol rules. This is treated as a connection
            error: h2 emits a GOAWAY frame and raises the error to the
            caller of ``receive_data``.
        :raises Exception: Any other exception is treated as an application
            error. h2 wraps it in
            :class:`ExtensionApplicationError
            <h2.exceptions.ExtensionApplicationError>` and raises it without
            terminating the connection or changing any core connection
            state.
        """
        raise NotImplementedError()
