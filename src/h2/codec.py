"""
h2/codec
~~~~~~~~

Pluggable HPACK codec support.

This module defines the minimal protocols that HPACK encoder and decoder
implementations must satisfy to be used with
:class:`H2Connection <h2.connection.H2Connection>`, along with the default
factories backed by the ``hpack`` package.

Codecs are stateful: the HPACK dynamic table is shared by every header block
on a connection, so each connection must be given its own encoder and
decoder instances. To catch factories that accidentally hand out shared
instances, every codec produced by a factory is registered while its
connection is alive, and handing the same instance to two live connections
raises :class:`ValueError`.
"""
from __future__ import annotations

import weakref
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from hpack.hpack import Decoder, Encoder

if TYPE_CHECKING:  # pragma: no cover
    from hpack.struct import Header, HeaderWeaklyTyped


@runtime_checkable
class EncoderProtocol(Protocol):
    """
    The minimal interface an HPACK encoder must provide.

    ``header_table_size`` is the outbound dynamic table size, in bytes. h2
    sets it when the peer's SETTINGS_HEADER_TABLE_SIZE is acknowledged.
    """

    @property
    def header_table_size(self) -> int:
        """
        The current maximum size of the dynamic table, in bytes.
        """
        ...

    @header_table_size.setter
    def header_table_size(self, value: int) -> None:
        """
        Update the maximum size of the dynamic table.
        """
        ...

    def encode(self, headers: Iterable[HeaderWeaklyTyped]) -> bytes:
        """
        Encode a header block, updating the dynamic table.
        """
        ...


@runtime_checkable
class DecoderProtocol(Protocol):
    """
    The minimal interface an HPACK decoder must provide.

    ``max_header_list_size`` is the inbound header list limit, in bytes,
    advertised to the peer via SETTINGS_MAX_HEADER_LIST_SIZE.
    ``max_allowed_table_size`` caps the dynamic table size the peer may
    ask this decoder to use, reflecting SETTINGS_HEADER_TABLE_SIZE.
    """

    @property
    def max_header_list_size(self) -> int:
        """
        The maximum decompressed size of a header list, in bytes.
        """
        ...

    @max_header_list_size.setter
    def max_header_list_size(self, value: int) -> None:
        """
        Update the maximum decompressed header list size.
        """
        ...

    @property
    def max_allowed_table_size(self) -> int:
        """
        The maximum dynamic table size the peer may select, in bytes.
        """
        ...

    @max_allowed_table_size.setter
    def max_allowed_table_size(self, value: int) -> None:
        """
        Update the maximum dynamic table size the peer may select.
        """
        ...

    def decode(self, data: bytes, raw: bool = False) -> Iterable[Header]:
        """
        Decode a header block, updating the dynamic table.
        """
        ...


EncoderFactory = Callable[[], EncoderProtocol]
DecoderFactory = Callable[[], DecoderProtocol]


def default_encoder_factory() -> Encoder:
    """
    Build the default HPACK encoder, backed by the ``hpack`` package.
    """
    return Encoder()


def default_decoder_factory() -> Decoder:
    """
    Build the default HPACK decoder, backed by the ``hpack`` package.
    """
    return Decoder()


# Registries of codec instances currently owned by live connections. Used to
# detect factories that return the same stateful codec to more than one
# connection, which would corrupt the HPACK dynamic tables of both.
_active_encoders: weakref.WeakSet[EncoderProtocol] = weakref.WeakSet()
_active_decoders: weakref.WeakSet[DecoderProtocol] = weakref.WeakSet()

_C = TypeVar("_C")


def _claim_codec(instance: _C,
                 registry: weakref.WeakSet[_C],
                 kind: str) -> None:
    """
    Register a codec instance as owned by a connection, rejecting instances
    that are already owned by another live connection.
    """
    try:
        already_claimed = instance in registry
        if not already_claimed:
            registry.add(instance)
    except TypeError:
        # Instances that do not support weak references cannot be tracked:
        # their factories are trusted not to share them.
        return

    if already_claimed:
        msg = (
            f"{kind} factory returned an instance that is already in use by "
            f"another H2Connection: HPACK codecs are stateful and must not "
            f"be shared across connections"
        )
        raise ValueError(msg)


def build_encoder(factory: EncoderFactory) -> EncoderProtocol:
    """
    Build an encoder from a factory, validating it and claiming it for the
    exclusive use of one connection.
    """
    encoder = factory()
    if not isinstance(encoder, EncoderProtocol):
        msg = (
            f"encoder factory returned {encoder!r}, which does not satisfy "
            f"the EncoderProtocol interface"
        )
        raise TypeError(msg)
    _claim_codec(encoder, _active_encoders, "encoder")
    return encoder


def build_decoder(factory: DecoderFactory) -> DecoderProtocol:
    """
    Build a decoder from a factory, validating it and claiming it for the
    exclusive use of one connection.
    """
    decoder = factory()
    if not isinstance(decoder, DecoderProtocol):
        msg = (
            f"decoder factory returned {decoder!r}, which does not satisfy "
            f"the DecoderProtocol interface"
        )
        raise TypeError(msg)
    _claim_codec(decoder, _active_decoders, "decoder")
    return decoder
