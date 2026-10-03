"""
HTTP/2 extension frame support.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .events import Event


class ExtensionFrameScope(Enum):
    """
    Identifies whether an extension frame belongs to a connection or a stream.
    """

    CONNECTION = 0
    STREAM = 1


@dataclass(frozen=True, slots=True)
class ReceivedExtensionFrame:
    """
    An immutable view of an extension frame delivered to a handler.
    """

    frame_type: int
    stream_id: int
    flags: int
    payload: bytes


class ExtensionFrameHandler(ABC):
    """
    Handles one privately negotiated HTTP/2 extension frame type.
    """

    @property
    @abstractmethod
    def frame_type(self) -> int:
        """The extension frame type handled by this handler."""

    @property
    @abstractmethod
    def scope(self) -> ExtensionFrameScope:
        """Whether frames must be on stream zero or an existing stream."""

    @property
    @abstractmethod
    def max_payload_size(self) -> int:
        """The maximum accepted payload size in bytes."""

    @abstractmethod
    def receive_frame(
        self,
        frame: ReceivedExtensionFrame,
    ) -> list[Event]:
        """
        Handle an inbound extension frame and return h2 events.
        """
