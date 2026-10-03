"""
Tests for the pluggable HPACK encoder/decoder factory extension point.

The codecs are the formal way to swap the HPACK implementation used by an
H2Connection without reaching into its private attributes. These tests
exercise:

* the minimal codec protocols and the default bundled factories;
* factory call counts and the limits passed in at construction time;
* synchronisation of dynamic table size and header list size limits at
  initialisation and around SETTINGS/SETTINGS ACK;
* byte-for-byte compatibility of a recording hpack wrapper with a plain
  connection, including dynamic-table reuse across two request rounds;
* rejection of codecs shared across connections (and the explicit opt-in);
* translation of arbitrary codec errors into public h2 exceptions, without
  partially decoded headers leaking into events.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.exceptions
import h2.settings

from hpack.exceptions import HPACKDecodingError, OversizedHeaderListError

from . import helpers


#: Headers large/rep... enough to populate the dynamic table meaningfully.
EXAMPLE_REQUEST_HEADERS = [
    (":method", "GET"),
    (":scheme", "https"),
    (":authority", "example.com"),
    (":path", "/some/resource/path"),
    ("user-agent", "h2-test-client/1.0"),
    ("accept", "*/*"),
]


class RecordingEncoder:
    """
    Wraps a bundled hpack Encoder and records every interaction.
    """

    def __init__(self, header_table_size: int) -> None:
        from hpack.hpack import Encoder

        self.calls: list[tuple[str, object]] = []
        self.calls.append(("init", header_table_size))
        self._encoder = Encoder()
        # Honour the limit handed to the factory, exactly like a conforming
        # implementation should.
        self.header_table_size = header_table_size
        self.encode_calls = 0

    @property
    def header_table_size(self) -> int:
        return self._encoder.header_table_size

    @header_table_size.setter
    def header_table_size(self, value: int) -> None:
        self.calls.append(("set_header_table_size", value))
        self._encoder.header_table_size = value

    def encode(self, headers):
        self.encode_calls += 1
        # Materialise before recording: h2 may pass a one-shot iterator, which
        # tuple() would otherwise consume before hpack sees it.
        header_list = list(headers)
        self.calls.append(("encode", len(header_list)))
        return self._encoder.encode(header_list)


class RecordingDecoder:
    """
    Wraps a bundled hpack Decoder and records every interaction.
    """

    def __init__(self, max_header_list_size, max_table_size) -> None:
        from hpack.hpack import Decoder

        self.calls: list[tuple[str, object]] = []
        self.calls.append(("init", (max_header_list_size, max_table_size)))
        self._decoder = Decoder(
            max_header_list_size=max_header_list_size
            if max_header_list_size is not None
            else 65536,
        )
        self.max_allowed_table_size = max_table_size
        self.decode_calls = 0

    @property
    def header_table_size(self) -> int:
        return self._decoder.header_table_size

    @property
    def max_header_list_size(self):
        return self._decoder.max_header_list_size

    @max_header_list_size.setter
    def max_header_list_size(self, value) -> None:
        self.calls.append(("set_max_header_list_size", value))
        self._decoder.max_header_list_size = value

    @property
    def max_allowed_table_size(self) -> int:
        return self._decoder.max_allowed_table_size

    @max_allowed_table_size.setter
    def max_allowed_table_size(self, value: int) -> None:
        self.calls.append(("set_max_allowed_table_size", value))
        self._decoder.max_allowed_table_size = value

    def decode(self, data, raw=True):
        self.decode_calls += 1
        self.calls.append(("decode", len(data)))
        return self._decoder.decode(data, raw=raw)


def make_config(client_side, *, encoder_factory=None, decoder_factory=None):
    return h2.config.H2Configuration(
        client_side=client_side,
        header_encoder_factory=encoder_factory,
        header_decoder_factory=decoder_factory,
    )


def make_recording_pair():
    """
    Build (client_config, server_config, encoders-dict, decoders-dict) where
    each factory records the single object it creates.
    """
    encoders = []
    decoders = []

    def encoder_factory(size):
        encoder = RecordingEncoder(size)
        encoders.append(encoder)
        return encoder

    def decoder_factory(max_header_list_size, max_table_size):
        decoder = RecordingDecoder(max_header_list_size, max_table_size)
        decoders.append(decoder)
        return decoder

    return (
        make_config(True, encoder_factory=encoder_factory,
                    decoder_factory=decoder_factory),
        make_config(False, encoder_factory=encoder_factory,
                    decoder_factory=decoder_factory),
        encoders,
        decoders,
    )


class TestDefaultFactories:
    def test_configuration_exposes_default_factories(self) -> None:
        from h2.hpack_codecs import (
            default_decoder_factory,
            default_encoder_factory,
        )

        config = h2.config.H2Configuration()
        assert config.header_encoder_factory is default_encoder_factory
        assert config.header_decoder_factory is default_decoder_factory

    def test_default_encoder_factory_applies_table_size(self) -> None:
        from h2.hpack_codecs import default_encoder_factory
        from hpack.hpack import Encoder

        encoder = default_encoder_factory(256)
        assert isinstance(encoder, Encoder)
        assert encoder.header_table_size == 256

    @pytest.mark.parametrize("max_header_list_size", [None, 1024])
    def test_default_decoder_factory_applies_limits(
        self, max_header_list_size,
    ) -> None:
        from h2.hpack_codecs import default_decoder_factory
        from hpack.hpack import Decoder

        decoder = default_decoder_factory(max_header_list_size, 512)
        assert isinstance(decoder, Decoder)
        assert decoder.max_allowed_table_size == 512
        if max_header_list_size is not None:
            assert decoder.max_header_list_size == 1024

    def test_factories_return_distinct_objects(self) -> None:
        from h2.hpack_codecs import (
            default_decoder_factory,
            default_encoder_factory,
        )

        assert default_encoder_factory(4096) is not default_encoder_factory(4096)
        assert (
            default_decoder_factory(65536, 4096)
            is not default_decoder_factory(65536, 4096)
        )


class TestConfigurationValidation:
    @pytest.mark.parametrize("bad_value", [1, "factory", object()])
    def test_encoder_factory_must_be_callable_or_none(self, bad_value) -> None:
        with pytest.raises(ValueError):
            h2.config.H2Configuration(header_encoder_factory=bad_value)

        config = h2.config.H2Configuration()
        with pytest.raises(ValueError):
            config.header_encoder_factory = bad_value

    @pytest.mark.parametrize("bad_value", [1, "factory", object()])
    def test_decoder_factory_must_be_callable_or_none(self, bad_value) -> None:
        with pytest.raises(ValueError):
            h2.config.H2Configuration(header_decoder_factory=bad_value)

        config = h2.config.H2Configuration()
        with pytest.raises(ValueError):
            config.header_decoder_factory = bad_value

    def test_factories_can_be_replaced_by_attribute(self) -> None:
        config = h2.config.H2Configuration()

        def encoder_factory(size):
            return RecordingEncoder(size)

        def decoder_factory(max_header_list_size, max_table_size):
            return RecordingDecoder(max_header_list_size, max_table_size)

        config.header_encoder_factory = encoder_factory
        config.header_decoder_factory = decoder_factory
        assert config.header_encoder_factory is encoder_factory
        assert config.header_decoder_factory is decoder_factory

    def test_setting_factory_to_none_restores_default(self) -> None:
        from h2.hpack_codecs import (
            default_decoder_factory,
            default_encoder_factory,
        )

        config = h2.config.H2Configuration(
            header_encoder_factory=lambda s: RecordingEncoder(s),
            header_decoder_factory=lambda m, t: RecordingDecoder(m, t),
        )
        config.header_encoder_factory = None
        config.header_decoder_factory = None
        assert config.header_encoder_factory is default_encoder_factory
        assert config.header_decoder_factory is default_decoder_factory


class TestFactoryInvocation:
    def test_factories_called_once_per_direction_with_initial_limits(self) -> None:
        client_config, server_config, encoders, decoders = (
            make_recording_pair()
        )

        client = h2.connection.H2Connection(config=client_config)
        server = h2.connection.H2Connection(config=server_config)

        # Exactly one encoder and one decoder per connection.
        assert len(encoders) == 2
        assert len(decoders) == 2

        # The encoder receives the *remote* default HEADER_TABLE_SIZE.
        assert all(
            ("init", 4096) in encoder.calls for encoder in encoders
        )
        # The decoder receives local MAX_HEADER_LIST_SIZE and HEADER_TABLE_SIZE.
        assert all(
            ("init", (65536, 4096)) in decoder.calls for decoder in decoders
        )

        # h2 also re-applies the limits directly after construction.
        assert all(
            ("set_header_table_size", 4096) in encoder.calls
            for encoder in encoders
        )
        assert all(
            ("set_max_header_list_size", 65536) in decoder.calls
            for decoder in decoders
        )
        assert all(
            ("set_max_allowed_table_size", 4096) in decoder.calls
            for decoder in decoders
        )

        # The built codecs are what the connection actually uses.
        assert client.encoder is encoders[0]
        assert client.decoder is decoders[0]
        assert server.encoder is encoders[1]
        assert server.decoder is decoders[1]

    def test_encoder_result_must_satisfy_protocol(self) -> None:
        class BrokenEncoder:
            # Missing header_table_size attribute and encode() method.
            pass

        with pytest.raises(TypeError):
            h2.connection.H2Connection(config=make_config(
                True, encoder_factory=lambda s: BrokenEncoder(),
            ))

    def test_decoder_result_must_satisfy_protocol(self) -> None:
        class BrokenDecoder:
            # Missing decode() and the limit attributes.
            pass

        with pytest.raises(TypeError):
            h2.connection.H2Connection(config=make_config(
                True, decoder_factory=lambda m, t: BrokenDecoder(),
            ))

    def test_factory_exception_propagates(self) -> None:
        def boom(size):
            raise RuntimeError("factory exploded")

        with pytest.raises(RuntimeError, match="factory exploded"):
            h2.connection.H2Connection(config=make_config(
                True, encoder_factory=boom,
            ))


class TestLimitSynchronisation:
    def _exchange_settings(self, frame_factory, client, server, values):
        """
        Have the server advertise ``values`` and complete the ACK round trip.
        """
        server.update_settings(values)
        client.receive_data(server.data_to_send())
        # receive_data emits the client's SETTINGS ACK automatically; forward it
        # back so the server treats its local settings as acknowledged.
        server.receive_data(client.data_to_send())

    def test_remote_header_table_size_reaches_encoder(
        self, frame_factory,
    ) -> None:
        client_config, server_config, encoders, decoders = (
            make_recording_pair()
        )
        client = h2.connection.H2Connection(config=client_config)
        server = h2.connection.H2Connection(config=server_config)
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        self._exchange_settings(
            frame_factory, client, server,
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 256},
        )

        assert client.encoder.header_table_size == 256
        assert ("set_header_table_size", 256) in encoders[0].calls

    def test_local_header_limits_reach_decoder_after_ack(
        self, frame_factory,
    ) -> None:
        client_config, server_config, encoders, decoders = (
            make_recording_pair()
        )
        client = h2.connection.H2Connection(config=client_config)
        server = h2.connection.H2Connection(config=server_config)
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        self._exchange_settings(
            frame_factory, client, server,
            {
                h2.settings.SettingCodes.HEADER_TABLE_SIZE: 512,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 9000,
            },
        )

        # The server's *own* settings, acknowledged by the client, constrain
        # its decoder (but only after the ACK arrives).
        assert server.decoder.max_allowed_table_size == 512
        assert server.decoder.max_header_list_size == 9000
        assert ("set_max_allowed_table_size", 512) in decoders[1].calls
        assert ("set_max_header_list_size", 9000) in decoders[1].calls

        # The client encoder mirrors the server's advertised table size.
        assert client.encoder.header_table_size == 512

    def test_decoder_table_size_unaffected_before_ack(
        self, frame_factory,
    ) -> None:
        client_config, server_config, _, _ = make_recording_pair()
        client = h2.connection.H2Connection(config=client_config)
        server = h2.connection.H2Connection(config=server_config)
        client.initiate_connection()
        server.receive_data(client.data_to_send())

        # Server proposes a change but the client has not yet acknowledged it.
        server.update_settings(
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 256},
        )
        client.receive_data(server.data_to_send())

        # Local settings are only applied once acked: decoder keeps 4096.
        assert server.decoder.header_table_size == 4096


class TestRoundTripAndByteCompatibility:
    def _recorded_connections(self):
        client_config, server_config, encoders, decoders = (
            make_recording_pair()
        )
        client = h2.connection.H2Connection(config=client_config)
        server = h2.connection.H2Connection(config=server_config)
        client.initiate_connection()
        server.receive_data(client.data_to_send())
        server.data_to_send()  # drain server settings/ACK
        return client, server, encoders, decoders

    def _plain_connections(self):
        client = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True),
        )
        server = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False),
        )
        client.initiate_connection()
        server.receive_data(client.data_to_send())
        server.data_to_send()
        return client, server

    def test_two_request_rounds_decode_and_reuse_dynamic_table(self) -> None:
        client, server, encoders, decoders = self._recorded_connections()

        decoded_rounds = []
        for round_index in range(2):
            client.send_headers(
                1 + 2 * round_index,
                EXAMPLE_REQUEST_HEADERS,
                end_stream=True,
            )
            wire = client.data_to_send()
            events = server.receive_data(wire)
            request = next(
                e for e in events
                if isinstance(e, h2.events.RequestReceived)
            )
            decoded_rounds.append(request.headers)

        assert len(decoded_rounds[0]) == len(EXAMPLE_REQUEST_HEADERS)
        assert decoded_rounds[0] == decoded_rounds[1]
        # Two encodes on the client, two decodes on the server.
        assert encoders[0].encode_calls == 2
        assert decoders[1].decode_calls == 2
        # Each connection's factory built exactly one codec.
        assert len(encoders) == 2
        assert len(decoders) == 2

    def test_wrapper_is_byte_compatible_with_plain_connection(self) -> None:
        recorded_client, recorded_server, _, _ = self._recorded_connections()
        plain_client, plain_server = self._plain_connections()

        recorded_blocks = []
        plain_blocks = []
        for round_index in range(2):
            recorded_client.send_headers(
                1 + 2 * round_index,
                EXAMPLE_REQUEST_HEADERS,
                end_stream=True,
            )
            plain_client.send_headers(
                1 + 2 * round_index,
                EXAMPLE_REQUEST_HEADERS,
                end_stream=True,
            )
            recorded_blocks.append(recorded_client.data_to_send())
            plain_blocks.append(plain_client.data_to_send())

        # Byte-for-byte identical output from the wrapper and the default
        # hpack encoder.
        assert recorded_blocks == plain_blocks

        # And the plain server's bytes decode on the recorded server too,
        # proving wire interoperability in both directions.
        for round_index, block in enumerate(plain_blocks):
            events = recorded_server.receive_data(block)
            request = next(
                e for e in events
                if isinstance(e, h2.events.RequestReceived)
            )
            assert request.headers == [
                (name.encode(), value.encode())
                for name, value in EXAMPLE_REQUEST_HEADERS
            ]

    def test_second_round_is_smaller_through_dynamic_table(self) -> None:
        client, server, _, _ = self._recorded_connections()

        sizes = []
        for round_index in range(2):
            client.send_headers(
                1 + 2 * round_index,
                EXAMPLE_REQUEST_HEADERS,
                end_stream=True,
            )
            sizes.append(len(client.data_to_send()))

        # Repeated headers are indexed after the first round.
        assert sizes[1] < sizes[0]


class TestSharedStateProtection:
    def test_shared_encoder_without_opt_in_is_rejected(self) -> None:
        shared = RecordingEncoder(4096)
        config_a = h2.config.H2Configuration(
            header_encoder_factory=lambda s: shared,
        )
        config_b = h2.config.H2Configuration(
            header_encoder_factory=lambda s: shared,
        )

        h2.connection.H2Connection(config=config_a)
        with pytest.raises(ValueError, match="must not be shared"):
            h2.connection.H2Connection(config=config_b)

    def test_shared_decoder_without_opt_in_is_rejected(self) -> None:
        shared = RecordingDecoder(65536, 4096)
        config_a = h2.config.H2Configuration(
            header_decoder_factory=lambda m, t: shared,
        )
        config_b = h2.config.H2Configuration(
            header_decoder_factory=lambda m, t: shared,
        )

        h2.connection.H2Connection(config=config_a)
        with pytest.raises(ValueError, match="must not be shared"):
            h2.connection.H2Connection(config=config_b)

    def test_non_weakrefable_codec_is_accepted(self) -> None:
        # Instances of int subclasses cannot be weakly referenced. h2 cannot
        # track sharing for them, but they still satisfy the protocol and must
        # work.
        class SolidEncoder(int):
            header_table_size = 4096

            def encode(self, headers):
                return b""

        class SolidDecoder(int):
            max_header_list_size = 65536
            max_allowed_table_size = 4096

            def decode(self, data, raw=True):
                return []

        config = h2.config.H2Configuration(
            header_encoder_factory=lambda s: SolidEncoder(),
            header_decoder_factory=lambda m, t: SolidDecoder(),
        )
        first = h2.connection.H2Connection(config=config)
        second = h2.connection.H2Connection(config=config)
        assert isinstance(first.encoder, SolidEncoder)
        assert isinstance(second.decoder, SolidDecoder)

    def test_shared_codec_is_allowed_after_explicit_opt_in(self) -> None:
        shared_encoder = RecordingEncoder(4096)
        shared_encoder.h2_allows_shared_state = True
        shared_decoder = RecordingDecoder(65536, 4096)
        shared_decoder.h2_allows_shared_state = True

        config_a = h2.config.H2Configuration(
            header_encoder_factory=lambda s: shared_encoder,
            header_decoder_factory=lambda m, t: shared_decoder,
        )
        config_b = h2.config.H2Configuration(
            header_encoder_factory=lambda s: shared_encoder,
            header_decoder_factory=lambda m, t: shared_decoder,
        )

        h2.connection.H2Connection(config=config_a)
        h2.connection.H2Connection(config=config_b)

    def test_codec_can_be_reused_after_owning_connection_is_collected(self) -> None:
        import gc

        # A factory returning fresh objects can serve many connections.
        fresh_config = h2.config.H2Configuration(
            header_encoder_factory=lambda s: RecordingEncoder(s),
        )
        first_connection = h2.connection.H2Connection(config=fresh_config)
        first_codec = first_connection.encoder
        second_connection = h2.connection.H2Connection(config=fresh_config)
        assert second_connection.encoder is not first_codec

        # Once every live owner of a particular codec object is gone, that
        # object may legitimately be handed to a brand new connection.
        codec = RecordingEncoder(4096)
        reuse_config = h2.config.H2Configuration(
            header_encoder_factory=lambda s: codec,
        )
        owner = h2.connection.H2Connection(config=reuse_config)
        assert owner.encoder is codec
        del owner
        gc.collect()

        new_owner = h2.connection.H2Connection(config=reuse_config)
        assert new_owner.encoder is codec


class TestErrorMapping:
    def _server_with_decoder(self, decoder):
        frame_factory = helpers.FrameFactory()
        server = h2.connection.H2Connection(config=h2.config.H2Configuration(
            client_side=False, header_decoder_factory=lambda m, t: decoder,
        ))
        server.receive_data(frame_factory.preamble())
        server.clear_outbound_data_buffer()
        headers_frame = frame_factory.build_headers_frame(
            [(b":method", b"GET"), (b":path", b"/")],
        )
        return server, headers_frame

    def test_decoder_arbitrary_error_becomes_protocol_error(self) -> None:
        class ExplodingDecoder:
            max_allowed_table_size = 4096
            max_header_list_size = 65536

            def decode(self, data, raw=True):
                raise RuntimeError("boom")

        server, frame = self._server_with_decoder(ExplodingDecoder())
        with pytest.raises(h2.exceptions.ProtocolError, match="boom"):
            server.receive_data(frame.serialize())

    def test_hpack_decoding_error_becomes_protocol_error(self) -> None:
        class HPACKFailingDecoder:
            max_allowed_table_size = 4096
            max_header_list_size = 65536

            def decode(self, data, raw=True):
                raise HPACKDecodingError("bad hpack")

        server, frame = self._server_with_decoder(HPACKFailingDecoder())
        with pytest.raises(h2.exceptions.ProtocolError, match="bad hpack"):
            server.receive_data(frame.serialize())

    def test_oversized_list_becomes_denial_of_service(self) -> None:
        class BombDecoder:
            max_allowed_table_size = 4096
            max_header_list_size = 65536

            def decode(self, data, raw=True):
                raise OversizedHeaderListError("header list too large")

        server, frame = self._server_with_decoder(BombDecoder())
        with pytest.raises(
            h2.exceptions.DenialOfServiceError, match="Oversized",
        ):
            server.receive_data(frame.serialize())

    def test_partial_decoded_headers_do_not_escape(self) -> None:
        class PartialDecoder:
            max_allowed_table_size = 4096
            max_header_list_size = 65536

            def decode(self, data, raw=True):
                yield (b":method", b"GET")
                yield (b":path", b"/")
                raise RuntimeError("mid-block failure")

        server, frame = self._server_with_decoder(PartialDecoder())
        with pytest.raises(h2.exceptions.ProtocolError, match="mid-block"):
            server.receive_data(frame.serialize())

        # The stream must not carry the headers that were decoded before the
        # failure.
        assert server.streams.get(1) is None
        # Only the mandatory GOAWAY may be emitted, never a RequestReceived.
        outbound = server.data_to_send()
        # GOAWAY frame type is 0x7.
        assert outbound[3:4] == b"\x07" if outbound else True

    def test_encoder_arbitrary_error_becomes_protocol_error(self) -> None:
        class ExplodingEncoder:
            header_table_size = 4096

            def encode(self, headers):
                raise RuntimeError("cannot encode")

        client = h2.connection.H2Connection(config=h2.config.H2Configuration(
            header_encoder_factory=lambda s: ExplodingEncoder(),
        ))
        client.initiate_connection()
        with pytest.raises(h2.exceptions.ProtocolError, match="cannot encode"):
            client.send_headers(
                1, [(b":method", b"GET"), (b":path", b"/")],
            )

    def test_encoder_table_size_error_becomes_protocol_error(
        self, frame_factory,
    ) -> None:
        class BadResizeEncoder(RecordingEncoder):
            def __init__(self, size):
                super().__init__(size)

            @RecordingEncoder.header_table_size.setter
            def header_table_size(self, value):
                if value == 256:
                    raise RuntimeError("unsupported size")
                RecordingEncoder.header_table_size.fset(self, value)

        client = h2.connection.H2Connection(config=h2.config.H2Configuration(
            header_encoder_factory=BadResizeEncoder,
        ))
        client.initiate_connection()

        settings_frame = frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.HEADER_TABLE_SIZE: 256},
        )
        with pytest.raises(h2.exceptions.ProtocolError, match="unsupported"):
            client.receive_data(settings_frame.serialize())
