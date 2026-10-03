"""
h2/config
~~~~~~~~~

Objects for controlling the configuration of the HTTP/2 stack.
"""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from .codec import default_decoder_factory, default_encoder_factory

if TYPE_CHECKING:  # pragma: no cover
    from .codec import DecoderFactory, EncoderFactory


class _BooleanConfigOption:
    """
    Descriptor for handling a boolean config option.  This will block
    attempts to set boolean config options to non-bools.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.attr_name = f"_{self.name}"

    def __get__(self, instance: Any, owner: Any) -> bool:
        return getattr(instance, self.attr_name)  # type: ignore

    def __set__(self, instance: Any, value: bool) -> None:
        if not isinstance(value, bool):
            msg = f"{self.name} must be a bool"
            raise ValueError(msg)  # noqa: TRY004
        setattr(instance, self.attr_name, value)


class DummyLogger:
    """
    A Logger object that does not actual logging, hence a DummyLogger.

    For the class the log operation is merely a no-op. The intent is to avoid
    conditionals being sprinkled throughout the h2 code for calls to
    logging functions when no logger is passed into the corresponding object.
    """

    def __init__(self, *vargs) -> None:  # type: ignore
        pass

    def debug(self, *vargs, **kwargs) -> None:  # type: ignore
        """
        No-op logging. Only level needed for now.
        """

    def trace(self, *vargs, **kwargs) -> None:  # type: ignore
        """
        No-op logging. Only level needed for now.
        """


class OutputLogger:
    """
    A Logger object that prints to stderr or any other file-like object.

    This class is provided for convenience and not part of the stable API.

    :param file: A file-like object passed to the print function.
        Defaults to ``sys.stderr``.
    :param trace: Enables trace-level output. Defaults to ``False``.
    """

    def __init__(self, file=None, trace_level=False) -> None:  # type: ignore
        super().__init__()
        self.file = file or sys.stderr
        self.trace_level = trace_level

    def debug(self, fmtstr, *args) -> None:  # type: ignore
        print(f"h2 (debug): {fmtstr % args}", file=self.file)

    def trace(self, fmtstr, *args) -> None:  # type: ignore
        if self.trace_level:
            print(f"h2 (trace): {fmtstr % args}", file=self.file)


class H2Configuration:
    """
    An object that controls the way a single HTTP/2 connection behaves.

    This object allows the users to customize behaviour. In particular, it
    allows users to enable or disable optional features, or to otherwise handle
    various unusual behaviours.

    This object has very little behaviour of its own: it mostly just ensures
    that configuration is self-consistent.

    :param client_side: Whether this object is to be used on the client side of
        a connection, or on the server side. Affects the logic used by the
        state machine, the default settings values, the allowable stream IDs,
        and several other properties. Defaults to ``True``.
    :type client_side: ``bool``

    :param header_encoding: Controls whether the headers emitted by this object
        in events are transparently decoded to ``unicode`` strings, and what
        encoding is used to do that decoding. This defaults to ``None``,
        meaning that headers will be returned as bytes. To automatically
        decode headers (that is, to return them as unicode strings), this can
        be set to the string name of any encoding, e.g. ``'utf-8'``.

        .. versionchanged:: 3.0.0
           Changed default value from ``'utf-8'`` to ``None``

    :type header_encoding: ``str``, ``False``, or ``None``

    :param validate_outbound_headers: Controls whether the headers emitted
        by this object are validated against the rules in RFC 7540.
        Disabling this setting will cause outbound header validation to
        be skipped, and allow the object to emit headers that may be illegal
        according to RFC 7540. Defaults to ``True``.
    :type validate_outbound_headers: ``bool``

    :param normalize_outbound_headers: Controls whether the headers emitted
        by this object are normalized before sending.  Disabling this setting
        will cause outbound header normalization to be skipped, and allow
        the object to emit headers that may be illegal according to
        RFC 7540. Defaults to ``True``.
    :type normalize_outbound_headers: ``bool``

    :param split_outbound_cookies: Controls whether the outbound cookie
        headers are split before sending or not. According to RFC 7540
        - 8.1.2.5 the outbound header cookie headers may be split to improve
        headers compression. Default is ``False``.
    :type split_outbound_cookies: ``bool``

    :param validate_inbound_headers: Controls whether the headers received
        by this object are validated against the rules in RFC 7540.
        Disabling this setting will cause inbound header validation to
        be skipped, and allow the object to receive headers that may be illegal
        according to RFC 7540. Defaults to ``True``.
    :type validate_inbound_headers: ``bool``

    :param normalize_inbound_headers: Controls whether the headers received by
        this object are normalized according to the rules of RFC 7540.
        Disabling this setting may lead to h2 emitting header blocks that
        some RFCs forbid, e.g. with multiple cookie fields.

        .. versionadded:: 3.0.0

    :type normalize_inbound_headers: ``bool``

    :param logger: A logger that conforms to the requirements for this module,
        those being no I/O and no context switches, which is needed in order
        to run in asynchronous operation.

        .. versionadded:: 2.6.0

    :type logger: ``logging.Logger``

    :param encoder_factory: A zero-argument callable that returns a fresh
        HPACK encoder satisfying
        :class:`EncoderProtocol <h2.codec.EncoderProtocol>`. Each
        :class:`H2Connection <h2.connection.H2Connection>` built with this
        configuration calls the factory exactly once, at construction time.
        The returned encoder must not be shared with any other live
        connection. Defaults to a factory backed by the ``hpack`` package.

        .. versionadded:: 4.5.0

    :type encoder_factory: ``Callable[[], h2.codec.EncoderProtocol]``

    :param decoder_factory: A zero-argument callable that returns a fresh
        HPACK decoder satisfying
        :class:`DecoderProtocol <h2.codec.DecoderProtocol>`. Each
        :class:`H2Connection <h2.connection.H2Connection>` built with this
        configuration calls the factory exactly once, at construction time.
        The returned decoder must not be shared with any other live
        connection. Defaults to a factory backed by the ``hpack`` package.

        .. versionadded:: 4.5.0

    :type decoder_factory: ``Callable[[], h2.codec.DecoderProtocol]``
    """

    client_side = _BooleanConfigOption("client_side")
    validate_outbound_headers = _BooleanConfigOption(
        "validate_outbound_headers",
    )
    normalize_outbound_headers = _BooleanConfigOption(
        "normalize_outbound_headers",
    )
    split_outbound_cookies = _BooleanConfigOption(
        "split_outbound_cookies",
    )
    validate_inbound_headers = _BooleanConfigOption(
        "validate_inbound_headers",
    )
    normalize_inbound_headers = _BooleanConfigOption(
        "normalize_inbound_headers",
    )

    def __init__(self,
                 client_side: bool = True,
                 header_encoding: bool | str | None = None,
                 validate_outbound_headers: bool = True,
                 normalize_outbound_headers: bool = True,
                 split_outbound_cookies: bool = False,
                 validate_inbound_headers: bool = True,
                 normalize_inbound_headers: bool = True,
                 logger: DummyLogger | OutputLogger | None = None,
                 encoder_factory: EncoderFactory | None = None,
                 decoder_factory: DecoderFactory | None = None) -> None:
        self.client_side = client_side
        self.header_encoding = header_encoding
        self.validate_outbound_headers = validate_outbound_headers
        self.normalize_outbound_headers = normalize_outbound_headers
        self.split_outbound_cookies = split_outbound_cookies
        self.validate_inbound_headers = validate_inbound_headers
        self.normalize_inbound_headers = normalize_inbound_headers
        self.logger = logger or DummyLogger(__name__)
        self.encoder_factory = encoder_factory
        self.decoder_factory = decoder_factory

    @property
    def header_encoding(self) -> bool | str | None:
        """
        Controls whether the headers emitted by this object in events are
        transparently decoded to ``unicode`` strings, and what encoding is used
        to do that decoding. This defaults to ``None``, meaning that headers
        will be returned as bytes. To automatically decode headers (that is, to
        return them as unicode strings), this can be set to the string name of
        any encoding, e.g. ``'utf-8'``.
        """
        return self._header_encoding

    @header_encoding.setter
    def header_encoding(self, value: bool | str | None) -> None:
        """
        Enforces constraints on the value of header encoding.
        """
        if not isinstance(value, (bool, str, type(None))):
            msg = "header_encoding must be bool, string, or None"
            raise ValueError(msg)  # noqa: TRY004
        if value is True:
            msg = "header_encoding cannot be True"
            raise ValueError(msg)
        self._header_encoding = value

    @property
    def encoder_factory(self) -> EncoderFactory:
        """
        The factory used to build the HPACK encoder for each connection
        created with this configuration. Defaults to a factory backed by
        the ``hpack`` package.

        .. versionadded:: 4.5.0
        """
        return self._encoder_factory

    @encoder_factory.setter
    def encoder_factory(self, value: EncoderFactory | None) -> None:
        """
        Enforces constraints on the encoder factory.
        """
        if value is None:
            value = default_encoder_factory
        if not callable(value):
            msg = "encoder_factory must be callable"
            raise ValueError(msg)  # noqa: TRY004
        self._encoder_factory = value

    @property
    def decoder_factory(self) -> DecoderFactory:
        """
        The factory used to build the HPACK decoder for each connection
        created with this configuration. Defaults to a factory backed by
        the ``hpack`` package.

        .. versionadded:: 4.5.0
        """
        return self._decoder_factory

    @decoder_factory.setter
    def decoder_factory(self, value: DecoderFactory | None) -> None:
        """
        Enforces constraints on the decoder factory.
        """
        if value is None:
            value = default_decoder_factory
        if not callable(value):
            msg = "decoder_factory must be callable"
            raise ValueError(msg)  # noqa: TRY004
        self._decoder_factory = value

