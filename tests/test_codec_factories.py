"""
Tests for the pluggable HPACK codec factories.

These tests use codecs that wrap the stock ``hpack`` encoder/decoder and
record every interaction, so that both the wiring (factory invocation,
parameter passing, settings synchronization) and the wire format (byte
compatibility with the default implementation) can be verified.
"""
from __future__ import annotations

import pytest
from hpack.hpack import Decoder, Encoder

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

REQUEST_HEADERS = [
    (":method", "GET"),
    (":path", "/"),
    (":authority", "example.com"),
    (":scheme", "https"),
    ("user-agent", "test-client/0.1.0"),
]

REQUEST_HEADERS_BYTES = [
    (b":method", b"GET"),
    (b":path", b"/"),
    (b":authority", b"example.com"),
    (b":scheme", b"https"),
    (b"user-agent", b"test-client/0.1.0"),
]


def _byte_pairs(headers):
    """
    Normalize a header iterable to a list of byte pairs for comparison.
    """
    return [
        (
            name if isinstance(name, bytes) else name.encode("ascii"),
            value if isinstance(value, bytes) else value.encode("ascii"),
        )
        for name, value in headers
    ]


class RecordingEncoder:
    """
    An EncoderProtocol implementation that delegates to ``hpack.Encoder``
    and records every call and attribute update.
    """

    def __init__(self) -> None:
        self._encoder = Encoder()
        self.log = []

    @property
    def header_table_size(self):
        return self._encoder.header_table_size

    @header_table_size.setter
    def header_table_size(self, value):
        self.log.append(("header_table_size", value))
        self._encoder.header_table_size = value

    def encode(self, headers):
        headers = list(headers)
        encoded = self._encoder.encode(headers)
        self.log.append(("encode", headers, encoded))
        return encoded

    def encoded_blocks(self):
        return [entry[2] for entry in self.log if entry[0] == "encode"]


class RecordingDecoder:
    """
    A DecoderProtocol implementation that delegates to ``hpack.Decoder``
    and records every call and attribute update.
    """

    def __init__(self) -> None:
        self._decoder = Decoder()
        self.log = []

    @property
    def max_header_list_size(self):
        return self._decoder.max_header_list_size

    @max_header_list_size.setter
    def max_header_list_size(self, value):
        self.log.append(("max_header_list_size", value))
        self._decoder.max_header_list_size = value

    @property
    def max_allowed_table_size(self):
        return self._decoder.max_allowed_table_size

    @max_allowed_table_size.setter
    def max_allowed_table_size(self, value):
        self.log.append(("max_allowed_table_size", value))
        self._decoder.max_allowed_table_size = value

    def decode(self, data, raw=False):
        self.log.append(("decode", data, raw))
        return self._decoder.decode(data, raw=raw)


class RecordingFactory:
    """
    A codec factory that counts its invocations and tracks the instances
    it produced.
    """

    def __init__(self, codec_type) -> None:
        self.codec_type = codec_type
        self.calls = 0
        self.instances = []

    def __call__(self):
        self.calls += 1
        instance = self.codec_type()
        self.instances.append(instance)
        return instance


def _pump(sender, receiver):
    """
    Move all pending data from one connection to the other, returning the
    bytes moved and the events they produced.
    """
    data = sender.data_to_send()
    events = receiver.receive_data(data) if data else []
    return data, events


def _run_session(client, server):
    """
    Run a scripted session: connection prefaces, a settings update that
    resizes both the dynamic table and the header list, and two requests
    carrying identical headers so the second reuses the dynamic table.

    Returns the list of byte chunks exchanged and the events emitted when
    the requests are received.
    """
    client.initiate_connection()
    server.initiate_connection()

    chunks = []
    chunks.append(_pump(client, server)[0])  # client preface + SETTINGS
    chunks.append(_pump(server, client)[0])  # server SETTINGS + ACK
    chunks.append(_pump(client, server)[0])  # client ACK

    client.update_settings({
        h2.settings.SettingCodes.HEADER_TABLE_SIZE: 256,
        h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 4096,
    })
    chunks.append(_pump(client, server)[0])  # new SETTINGS
    chunks.append(_pump(server, client)[0])  # ACK

    client.send_headers(1, REQUEST_HEADERS, end_stream=True)
    client.send_headers(3, REQUEST_HEADERS, end_stream=True)
    data, events = _pump(client, server)
    chunks.append(data)
    return chunks, events


def _recording_pair():
    """
    Build a client/server pair of connections wired to recording codecs,
    along with the four factories.
    """
    client_encoder_factory = RecordingFactory(RecordingEncoder)
    client_decoder_factory = RecordingFactory(RecordingDecoder)
    server_encoder_factory = RecordingFactory(RecordingEncoder)
    server_decoder_factory = RecordingFactory(RecordingDecoder)

    client = h2.connection.H2Connection(
        config=h2.config.H2Configuration(
            client_side=True,
            encoder_factory=client_encoder_factory,
            decoder_factory=client_decoder_factory,
        ),
    )
    server = h2.connection.H2Connection(
        config=h2.config.H2Configuration(
            client_side=False,
            encoder_factory=server_encoder_factory,
            decoder_factory=server_decoder_factory,
        ),
    )
    factories = (
        client_encoder_factory,
        client_decoder_factory,
        server_encoder_factory,
        server_decoder_factory,
    )
    return client, server, factories


class TestCodecFactories:
    """
    Tests for injecting HPACK codecs via ``H2Configuration``.
    """

    def test_factories_called_exactly_once_per_connection(self) -> None:
        """
        Each connection invokes each factory exactly once, at construction
        time, and owns the produced instances.
        """
        client, server, factories = _recording_pair()

        for factory in factories:
            assert factory.calls == 1

        assert client.encoder is factories[0].instances[0]
        assert client.decoder is factories[1].instances[0]
        assert server.encoder is factories[2].instances[0]
        assert server.decoder is factories[3].instances[0]

    def test_default_factories_produce_hpack_codecs(self) -> None:
        """
        Without explicit factories the connection uses the hpack-backed
        defaults.
        """
        config = h2.config.H2Configuration()
        conn = h2.connection.H2Connection(config=config)
        assert isinstance(conn.encoder, Encoder)
        assert isinstance(conn.decoder, Decoder)

    def test_initial_limits_synced_at_construction(self) -> None:
        """
        At initialization the codecs receive the limits implied by the
        initial settings in both directions.
        """
        client, _, factories = _recording_pair()
        encoder = factories[0].instances[0]
        decoder = factories[1].instances[0]

        # The encoder starts from the peer's default table size, and the
        # decoder from our advertised limits.
        assert ("header_table_size", 4096) in encoder.log
        assert (
            "max_header_list_size",
            h2.connection.H2Connection.DEFAULT_MAX_HEADER_LIST_SIZE,
        ) in decoder.log
        assert ("max_allowed_table_size", 4096) in decoder.log

    def test_settings_exchange_syncs_both_directions(self) -> None:
        """
        Exchanging settings updates the encoder of the peer receiving the
        SETTINGS frame and the decoder of the peer whose settings are
        acknowledged.
        """
        client, server, factories = _recording_pair()
        _run_session(client, server)

        client_encoder = factories[0].instances[0]
        client_decoder = factories[1].instances[0]
        server_encoder = factories[2].instances[0]

        # The server received the client's SETTINGS: its encoder must adopt
        # the new table size.
        assert ("header_table_size", 256) in server_encoder.log
        assert server_encoder.header_table_size == 256

        # The client's settings were ACKed: its decoder must adopt the
        # advertised limits.
        assert ("max_allowed_table_size", 256) in client_decoder.log
        assert ("max_header_list_size", 4096) in client_decoder.log
        assert client_decoder.max_allowed_table_size == 256
        assert client_decoder.max_header_list_size == 4096

        # The client never changed its encoder table size: the server kept
        # the default in its own settings.
        assert ("header_table_size", 4096) in client_encoder.log
        assert client_encoder.header_table_size == 4096

    def test_dynamic_table_reuse_and_parameter_passing(self) -> None:
        """
        Two identical requests reuse the dynamic table: the second encoded
        block is smaller, and the decoder receives each block with
        ``raw=True``.
        """
        client, server, factories = _recording_pair()
        _, events = _run_session(client, server)

        client_encoder = factories[0].instances[0]
        server_decoder = factories[3].instances[0]

        # The encoder saw exactly the headers we sent, twice.
        encode_calls = [
            entry for entry in client_encoder.log if entry[0] == "encode"
        ]
        assert len(encode_calls) == 2
        for _, headers, _ in encode_calls:
            assert _byte_pairs(headers) == REQUEST_HEADERS_BYTES

        # The second block is smaller: it indexes the first request's
        # dynamic table entries.
        first_block, second_block = client_encoder.encoded_blocks()
        assert 0 < len(second_block) < len(first_block)

        # The decoder received exactly those bytes, with raw=True.
        decode_calls = [
            entry for entry in server_decoder.log if entry[0] == "decode"
        ]
        assert decode_calls == [
            ("decode", first_block, True),
            ("decode", second_block, True),
        ]

        # And both requests arrived intact.
        received = [
            event for event in events
            if isinstance(event, h2.events.RequestReceived)
        ]
        assert [event.stream_id for event in received] == [1, 3]
        for event in received:
            assert event.headers == REQUEST_HEADERS_BYTES

    def test_byte_compatibility_with_default_codecs(self) -> None:
        """
        A session run through the recording codecs produces exactly the
        same bytes as the same session run through the default codecs.
        """
        client, server, _ = _recording_pair()
        recorded_chunks, _ = _run_session(client, server)

        reference_client = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True),
        )
        reference_server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False),
        )
        reference_chunks, _ = _run_session(reference_client, reference_server)

        assert recorded_chunks == reference_chunks
        assert any(recorded_chunks)  # the session actually exchanged data

    def test_shared_encoder_instance_rejected(self) -> None:
        """
        A factory that hands the same encoder to two live connections is
        detected and rejected.
        """
        shared = RecordingEncoder()
        config = h2.config.H2Configuration(encoder_factory=lambda: shared)
        h2.connection.H2Connection(config=config)
        with pytest.raises(ValueError, match="already in use"):
            h2.connection.H2Connection(config=config)

    def test_shared_decoder_instance_rejected(self) -> None:
        """
        A factory that hands the same decoder to two live connections is
        detected and rejected.
        """
        shared = RecordingDecoder()
        config = h2.config.H2Configuration(decoder_factory=lambda: shared)
        h2.connection.H2Connection(config=config)
        with pytest.raises(ValueError, match="already in use"):
            h2.connection.H2Connection(config=config)

    def test_factory_returning_invalid_codec_rejected(self) -> None:
        """
        Factories that return objects not satisfying the codec protocols
        fail fast at connection construction.
        """
        config = h2.config.H2Configuration(encoder_factory=lambda: object())
        with pytest.raises(TypeError, match="EncoderProtocol"):
            h2.connection.H2Connection(config=config)

        config = h2.config.H2Configuration(decoder_factory=lambda: object())
        with pytest.raises(TypeError, match="DecoderProtocol"):
            h2.connection.H2Connection(config=config)

    @pytest.mark.parametrize("option", ["encoder_factory", "decoder_factory"])
    def test_non_callable_factory_rejected(self, option) -> None:
        """
        Factories must be callable.
        """
        with pytest.raises(ValueError, match=f"{option} must be callable"):
            h2.config.H2Configuration(**{option: "not-callable"})

    def test_decoder_error_mapped_to_protocol_error(self) -> None:
        """
        Custom decoder exceptions surface as h2's public ProtocolError,
        not as the codec's private exception type.
        """
        class CustomDecodeError(Exception):
            pass

        class BrokenDecoder(RecordingDecoder):
            def decode(self, data, raw=False):
                raise CustomDecodeError("boom")

        server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=False,
                decoder_factory=RecordingFactory(BrokenDecoder),
            ),
        )
        client = h2.connection.H2Connection()
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        client.send_headers(1, REQUEST_HEADERS, end_stream=True)
        with pytest.raises(h2.exceptions.ProtocolError,
                           match="Error decoding header block"):
            server.receive_data(client.data_to_send())

    def test_lazy_decoder_cannot_leak_partial_headers(self) -> None:
        """
        A decoder that yields some headers and then fails must not expose
        the partially decoded block: decoding is completed eagerly inside
        the error translation layer.
        """
        class CustomDecodeError(Exception):
            pass

        class LazyBrokenDecoder(RecordingDecoder):
            def decode(self, data, raw=False):
                yield (b":method", b"GET")
                raise CustomDecodeError("boom")

        server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=False,
                decoder_factory=RecordingFactory(LazyBrokenDecoder),
            ),
        )
        client = h2.connection.H2Connection()
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        client.send_headers(1, REQUEST_HEADERS, end_stream=True)
        with pytest.raises(h2.exceptions.ProtocolError,
                           match="Error decoding header block"):
            server.receive_data(client.data_to_send())

        # No request event was emitted for the partial header block.
        assert 1 not in server.streams
        assert server.highest_inbound_stream_id == 0

    def test_encoder_error_mapped_to_protocol_error(self) -> None:
        """
        Custom encoder exceptions surface as h2's public ProtocolError.
        """
        class CustomEncodeError(Exception):
            pass

        class BrokenEncoder(RecordingEncoder):
            def encode(self, headers):
                raise CustomEncodeError("boom")

        client = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                encoder_factory=RecordingFactory(BrokenEncoder),
            ),
        )
        client.initiate_connection()
        with pytest.raises(h2.exceptions.ProtocolError,
                           match="Error encoding header block"):
            client.send_headers(1, REQUEST_HEADERS, end_stream=True)

    def test_oversized_header_list_still_maps_to_dos_error(self) -> None:
        """
        The hpack oversized-header-list error keeps its dedicated mapping
        to DenialOfServiceError when raised through a custom decoder.
        """
        from hpack.exceptions import OversizedHeaderListError

        class OversizedDecoder(RecordingDecoder):
            def decode(self, data, raw=False):
                raise OversizedHeaderListError("too large")

        server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=False,
                decoder_factory=RecordingFactory(OversizedDecoder),
            ),
        )
        client = h2.connection.H2Connection()
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        client.send_headers(1, REQUEST_HEADERS, end_stream=True)
        with pytest.raises(h2.exceptions.DenialOfServiceError,
                           match="Oversized header block"):
            server.receive_data(client.data_to_send())

    def test_non_weakrefable_codec_accepted(self) -> None:
        """
        Codecs that cannot be weakly referenced skip the shared-instance
        registry instead of failing.
        """
        class SlottedEncoder:
            __slots__ = ("_encoder",)

            def __init__(self) -> None:
                self._encoder = Encoder()

            @property
            def header_table_size(self):
                return self._encoder.header_table_size

            @header_table_size.setter
            def header_table_size(self, value):
                self._encoder.header_table_size = value

            def encode(self, headers):
                return self._encoder.encode(headers)

        factory = RecordingFactory(SlottedEncoder)
        config = h2.config.H2Configuration(encoder_factory=factory)
        first = h2.connection.H2Connection(config=config)
        second = h2.connection.H2Connection(config=config)
        assert factory.calls == 2
        assert first.encoder is not second.encoder
