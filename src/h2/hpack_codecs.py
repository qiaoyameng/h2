"""
h2/hpack_codecs
~~~~~~~~~~~~~~~

Formal extension points for pluggable HPACK implementations.

The :class:`~h2.config.H2Configuration` object accepts *encoder factory* and
*decoder factory* callables. Each new :class:`~h2.connection.H2Connection`
invokes these factories exactly once per direction and receives an isolated
codec object in return. This module defines the minimal protocol that those
objects must implement, the signatures of the factories themselves, and the
default factories that wrap the bundled :mod:`hpack` library.

Factories are called with the limits that apply to the direction they serve,
so that an implementation does not have to infer them from SETTINGS frames:

* ``encoder_factory(header_table_size)`` is called with the current value of
  the remote ``SETTINGS_HEADER_TABLE_SIZE`` (the maximum dynamic table size
  the encoder is allowed to use).
* ``decoder_factory(max_header_list_size, max_table_size)`` is called with
  the locally advertised ``SETTINGS_MAX_HEADER_LIST_SIZE`` and the local
  ``SETTINGS_HEADER_TABLE_SIZE`` (the maximum dynamic table size the remote
  encoder may use).

Factories MUST return a fresh codec object for each connection. A codec that
is handed to more than one connection shares HPACK dynamic-table state
between peers that cannot actually synchronise that state, which produces
silent stream corruption. Such sharing is detected and rejected unless the
returned object explicitly opts out by setting the attribute
``h2_allows_shared_state = True`` (a deliberate acknowledgement that the
implementation provides its own isolation guarantees, e.g. it is stateless).
"""
from __future__ import annotations

import weakref
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from hpack.exceptions import HPACKError, OversizedHeaderListError
from hpack.hpack import Decoder, Encoder

from .exceptions import DenialOfServiceError, ProtocolError

if TYPE_CHECKING:  # pragma: no cover
    from hpack.struct import Header


#: Attribute a codec object may set to ``True`` to acknowledge that it is
#: intentionally shared (or stateless) and exempt from the sharing check.
SHARED_STATE_OPT_IN_ATTR = "h2_allows_shared_state"


@runtime_checkable
class HeaderEncoder(Protocol):
    """
    The minimal protocol implemented by an HPACK *encoder*.

    An encoder turns a header list into an HPACK header block. The dynamic
    table it maintains is private to the connection it belongs to.
    """

    #: The maximum size, in octets, of the dynamic table the encoder is
    #: currently allowed to use. h2 keeps this synchronised with the remote
    #: peer's ``SETTINGS_HEADER_TABLE_SIZE``.
    header_table_size: int

    def encode(self, headers: Iterable[Header]) -> bytes:
        """
        Encode a header list into an HPACK header block.

        :param headers: An iterable of ``(name, value)`` header pairs (or
            :mod:`hpack` ``HeaderTuple`` objects). Names and values are
            bytestrings.
        """
        ...


@runtime_checkable
class HeaderDecoder(Protocol):
    """
    The minimal protocol implemented by an HPACK *decoder*.

    A decoder turns an HPACK header block back into a header list. The
    dynamic table it maintains is private to the connection it belongs to.
    """

    #: The maximum size, in octets, of the dynamic table the remote encoder
    #: is allowed to ask for. h2 keeps this synchronised with the local
    #: ``SETTINGS_HEADER_TABLE_SIZE``.
    max_allowed_table_size: int

    #: The maximum uncompressed size, in octets, of a header list h2 will
    #: accept. h2 keeps this synchronised with the local
    #: ``SETTINGS_MAX_HEADER_LIST_SIZE``. ``None`` means unlimited.
    max_header_list_size: int | None

    def decode(
        self,
        encoded_header_block: bytes,
        raw: bool = True,
    ) -> Iterable[Header]:
        """
        Decode an HPACK header block into an iterable of ``(name, value)``
        bytestring pairs.

        Implementations are always called with ``raw=True`` and must return
        bytestrings.
        """
        ...


#: A factory that builds an encoder for one connection.
EncoderFactory = Callable[[int], HeaderEncoder]

#: A factory that builds a decoder for one connection.
DecoderFactory = Callable[[int | None, int], HeaderDecoder]


def default_encoder_factory(header_table_size: int) -> Encoder:
    """
    Build a fresh bundled :class:`hpack.hpack.Encoder` configured to use at
    most ``header_table_size`` octets of dynamic table.

    This is the default value of
    :attr:`H2Configuration.header_encoder_factory
    <h2.config.H2Configuration.header_encoder_factory>`.
    """
    encoder = Encoder()
    encoder.header_table_size = header_table_size
    return encoder


def default_decoder_factory(
    max_header_list_size: int | None,
    max_table_size: int,
) -> Decoder:
    """
    Build a fresh bundled :class:`hpack.hpack.Decoder` configured with the
    given header list size limit and dynamic table size limit.

    This is the default value of
    :attr:`H2Configuration.header_decoder_factory
    <h2.config.H2Configuration.header_decoder_factory>`.
    """
    # hpack's Decoder takes a concrete integer (its own default is 65536).
    # None means unlimited: the bundled library always keeps its built-in
    # ceiling in that case, so pass its default through unchanged.
    decoder = Decoder(
        max_header_list_size=(
            max_header_list_size if max_header_list_size is not None else 65536
        ),
    )
    decoder.max_allowed_table_size = max_table_size
    return decoder


class _IdentityWeakRef(weakref.ref):
    """
    A weak reference that hashes and compares by the identity of its
    referent, so it can be used as a dict key tracking live objects.
    """

    __slots__ = ("_identity",)

    def __init__(self, ob: object) -> None:
        self._identity = id(ob)
        super().__init__(ob)

    def __hash__(self) -> int:
        return self._identity

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _IdentityWeakRef):
            return self._identity == other._identity
        return NotImplemented


class _LiveCodecRegistry:
    """
    Tracks which codec objects are currently owned by a live connection, so
    the same instance cannot be silently handed to two connections.

    Ownership is tracked against the *connection*: a codec object may remain
    referenced by user code after its connection is gone, and in that state
    it is safe to hand it to a new connection. A codec is considered shared
    only while another live connection still owns it.

    A codec carries per-connection HPACK dynamic-table state. Reusing one
    across live connections is only safe if the implementation is stateless
    or performs its own per-connection keying, and it advertises that fact
    via the ``h2_allows_shared_state`` opt-in.
    """

    def __init__(self) -> None:
        self._owners: dict[_IdentityWeakRef, weakref.ReferenceType[object]] = {}

    def register(self, codec: object, owner: object, kind: str) -> None:
        if getattr(codec, SHARED_STATE_OPT_IN_ATTR, False):
            return
        try:
            codec_ref = _IdentityWeakRef(codec)
        except TypeError:
            # Codecs that cannot be weakly referenced cannot be tracked;
            # the "fresh object per connection" contract still applies.
            return

        # Drop entries whose owning connection (or codec) has been collected.
        for key, owner_ref in list(self._owners.items()):
            if owner_ref() is None or key() is None:
                del self._owners[key]

        existing_owner = self._owners.get(codec_ref)
        if existing_owner is not None and existing_owner() is not None:
            msg = (
                f"The same {kind} object was returned by a factory for more "
                "than one H2Connection. HPACK codecs carry per-connection "
                "dynamic-table state and must not be shared. Return a fresh "
                "object from the factory, or set "
                f"{SHARED_STATE_OPT_IN_ATTR}=True on a deliberately "
                "stateless/shared implementation."
            )
            raise ValueError(msg)
        self._owners[codec_ref] = weakref.ref(owner)


_live_codecs = _LiveCodecRegistry()


def _check_codec(codec: object, protocol: type, kind: str) -> None:
    if not isinstance(codec, protocol):
        msg = (
            f"The {kind} factory returned an object that does not implement "
            f"the required {protocol.__name__} protocol: {codec!r}"
        )
        raise TypeError(msg)


def register_encoder(codec: object, owner: object) -> None:
    """
    Validate an encoder produced by a factory and record that ``owner`` now
    owns it.
    """
    _check_codec(codec, HeaderEncoder, "HPACK encoder")
    _live_codecs.register(codec, owner, "HPACK encoder")


def register_decoder(codec: object, owner: object) -> None:
    """
    Validate a decoder produced by a factory and record that ``owner`` now
    owns it.
    """
    _check_codec(codec, HeaderDecoder, "HPACK decoder")
    _live_codecs.register(codec, owner, "HPACK decoder")


def encode_headers(encoder: HeaderEncoder, headers: Iterable[Header]) -> bytes:
    """
    Encode a header list, mapping failures raised by a pluggable encoder to
    public h2 exceptions instead of leaking the underlying library's error
    types.
    """
    try:
        return encoder.encode(headers)
    except (ProtocolError, DenialOfServiceError):
        raise
    except Exception as e:  # noqa: BLE001 - any codec error is a protocol error
        msg = f"Error encoding header block: {e}"
        raise ProtocolError(msg) from e


def decode_headers(
    decoder: HeaderDecoder,
    encoded_header_block: bytes,
) -> list[Header]:
    """
    Decode an HPACK header block, translating codec exceptions into public h2
    exceptions.

    Decoding is fully consumed (the result is materialised into a list) before
    anything is returned: if decoding fails partway through a block, the
    partially decoded headers never escape as a "successful" event payload.
    """
    try:
        return list(decoder.decode(encoded_header_block, raw=True))
    except OversizedHeaderListError as e:
        # A header block larger than the advertised limit: the classic HPACK
        # bomb attack.
        msg = f"Oversized header block: {e}"
        raise DenialOfServiceError(msg) from e
    except (ProtocolError, DenialOfServiceError):
        raise
    except (HPACKError, IndexError, TypeError, UnicodeDecodeError) as e:
        # Older hpack releases (and third-party codecs) may raise any of
        # these when given malformed input.
        msg = f"Error decoding header block: {e}"
        raise ProtocolError(msg) from e
    except Exception as e:  # noqa: BLE001
        # A pluggable decoder must not leak its own error types through h2.
        msg = f"Error decoding header block: {e}"
        raise ProtocolError(msg) from e
