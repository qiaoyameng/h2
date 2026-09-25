"""
Test the RFC 8336 ORIGIN frame support.
"""
from __future__ import annotations

import itertools

import pytest

import h2.config
import h2.connection
import h2.events
import h2.exceptions
from h2.frame import OriginFrame


def origin_frame_bytes(body: bytes, stream_id: int = 0) -> bytes:
    """
    Serialize a raw ORIGIN frame from an unvalidated body. This allows tests
    to emit frames that OriginFrame itself would refuse to serialize.
    """
    header = len(body).to_bytes(3, "big")
    header += b"\x0c"  # ORIGIN frame type
    header += b"\x00"  # no flags
    header += stream_id.to_bytes(4, "big")
    return header + body


def origin_entry(origin: bytes) -> bytes:
    """
    Serialize a single Origin-Entry for an ORIGIN frame payload.
    """
    return len(origin).to_bytes(2, "big") + origin


class TestRFC8336Client:
    """
    Tests that the client supports receiving the RFC 8336 ORIGIN frame.
    """

    example_origins = [
        b"https://example.com",
        b"https://www.example.com",
        b"https://example.com:8443",
    ]

    def test_receiving_origin_frame(self, frame_factory) -> None:
        """
        An ORIGIN frame received on stream zero emits an OriginReceived event
        carrying all the asserted origins.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame(origins=self.example_origins)
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == self.example_origins

        # No data gets sent.
        assert not c.data_to_send()

    def test_origin_frame_updates_declared_origins(self, frame_factory) -> None:
        """
        Received ORIGIN frames are reflected in the connection-level view of
        the origins asserted by the server.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        assert c.declared_origins == frozenset()

        f = frame_factory.build_origin_frame(origins=self.example_origins)
        c.receive_data(f.serialize())

        assert c.declared_origins == frozenset(self.example_origins)

    def test_multiple_origin_frames_union(self, frame_factory) -> None:
        """
        The declared origins are the union of the origins asserted by all
        received ORIGIN frames.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        first = frame_factory.build_origin_frame(origins=self.example_origins[:2])
        second = frame_factory.build_origin_frame(origins=self.example_origins[2:])
        events = c.receive_data(first.serialize() + second.serialize())

        assert len(events) == 2
        assert all(isinstance(e, h2.events.OriginReceived) for e in events)
        assert events[0].origins == self.example_origins[:2]
        assert events[1].origins == self.example_origins[2:]
        assert c.declared_origins == frozenset(self.example_origins)

    def test_duplicate_origins_are_not_new_authorizations(self, frame_factory) -> None:
        """
        A server re-asserting an origin it already asserted does not change
        the set of declared origins.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        frames = [
            frame_factory.build_origin_frame(origins=self.example_origins[:2]),
            frame_factory.build_origin_frame(
                origins=[self.example_origins[0], self.example_origins[2]],
            ),
        ]
        events = c.receive_data(b"".join(f.serialize() for f in frames))

        # Each frame still emits an event describing exactly what it carried,
        # but the connection-level set is unchanged by the duplicate.
        assert len(events) == 2
        assert events[1].origins == [self.example_origins[0], self.example_origins[2]]
        assert c.declared_origins == frozenset(self.example_origins)

    def test_empty_origin_frame(self, frame_factory) -> None:
        """
        An ORIGIN frame with an empty payload asserts no origins and leaves
        the declared origin set unchanged.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        f = frame_factory.build_origin_frame(origins=[])
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.OriginReceived)
        assert events[0].origins == []
        assert c.declared_origins == frozenset()

    @pytest.mark.parametrize("origin", [
        b"https://example.com",
        b"http://example.com:8080",
        b"https://127.0.0.1",
        b"https://[::1]:8443",
        b"wss://example.com",
    ])
    def test_valid_origin_syntaxes(self, frame_factory, origin) -> None:
        """
        Origins that follow the RFC 6454 serialization are accepted.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        f = frame_factory.build_origin_frame(origins=[origin])
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert events[0].origins == [origin]

    @pytest.mark.parametrize("origin", [
        b"",  # empty origin entry
        b"example.com",  # missing scheme
        b"https://",  # missing host
        b"https://example.com/",  # trailing path
        b"https://example.com:99999",  # port out of range
        b"https://example.com:abc",  # non-numeric port
        b"https://exa mple.com",  # whitespace in host
        b"https://user@example.com",  # userinfo is not allowed
        "https://exämple.com".encode(),  # non-ASCIIASCII
    ])
    def test_invalid_origin_syntax(self, origin) -> None:
        """
        Origins that do not follow the RFC 6454 serialization are a
        connection error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        data = origin_frame_bytes(origin_entry(origin))
        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(data)

    def test_truncated_origin_length(self) -> None:
        """
        An ORIGIN frame whose payload ends part-way through an origin length
        is a connection error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        data = origin_frame_bytes(b"\x00")
        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(data)

    def test_truncated_origin(self) -> None:
        """
        An ORIGIN frame whose payload ends before a promised origin is
        complete is a connection error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        body = origin_entry(b"https://example.com")[:-3]
        data = origin_frame_bytes(body)
        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(data)

    @pytest.mark.parametrize("stream_id", [1, 3, 2**31 - 1])
    def test_origin_frame_on_stream_is_ignored(self, frame_factory, stream_id) -> None:
        """
        ORIGIN frames received on a stream other than stream zero are invalid
        and must be ignored.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame(
            origins=self.example_origins, stream_id=stream_id,
        )
        events = c.receive_data(f.serialize())

        assert not events
        assert c.declared_origins == frozenset()
        assert not c.data_to_send()

    @pytest.mark.parametrize("chunk_sizes", [
        [1],  # one byte at a time
        [3, 7, 2, 100],  # irregular chunks
        [1000],  # all at once
    ])
    def test_origin_frame_arbitrary_chunking(self, frame_factory, chunk_sizes) -> None:
        """
        Regardless of how the network chunks the data, the same events are
        emitted.
        """
        frames = [
            frame_factory.build_origin_frame(origins=self.example_origins[:2]),
            frame_factory.build_origin_frame(origins=self.example_origins[2:]),
        ]
        data = b"".join(f.serialize() for f in frames)

        c = h2.connection.H2Connection()
        c.initiate_connection()

        events = []
        offset = 0
        for size in itertools.cycle(chunk_sizes):
            if offset >= len(data):
                break
            events.extend(c.receive_data(data[offset:offset + size]))
            offset += size

        assert len(events) == 2
        assert events[0].origins == self.example_origins[:2]
        assert events[1].origins == self.example_origins[2:]
        assert c.declared_origins == frozenset(self.example_origins)

    def test_client_cannot_send_origin_frame(self) -> None:
        """
        Clients must not send ORIGIN frames.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_origin(self.example_origins)

        assert not c.data_to_send()


class TestRFC8336Server:
    """
    Tests that the server supports sending the RFC 8336 ORIGIN frame and
    ignores received ones.
    """

    server_config = h2.config.H2Configuration(client_side=False)

    example_origins = [
        b"https://example.com",
        b"https://www.example.com",
    ]

    def test_sending_origin_frame(self, frame_factory) -> None:
        """
        A server can send an ORIGIN frame that a client correctly receives.
        """
        server = h2.connection.H2Connection(config=self.server_config)
        server.initiate_connection()
        server.clear_outbound_data_buffer()

        server.send_origin(self.example_origins)
        data = server.data_to_send()

        client = h2.connection.H2Connection()
        client.initiate_connection()
        events = client.receive_data(data)

        assert len(events) == 1
        assert isinstance(events[0], h2.events.OriginReceived)
        assert events[0].origins == self.example_origins
        assert client.declared_origins == frozenset(self.example_origins)

    def test_sending_origin_frame_accepts_strings(self) -> None:
        """
        send_origin accepts origins as text strings as well as bytes.
        """
        server = h2.connection.H2Connection(config=self.server_config)
        server.initiate_connection()
        server.clear_outbound_data_buffer()

        server.send_origin(["https://example.com"])

        expected = OriginFrame(stream_id=0, origins=[b"https://example.com"])
        assert server.data_to_send() == expected.serialize()

    def test_sending_empty_origin_frame(self) -> None:
        """
        A server can send an ORIGIN frame with an empty payload.
        """
        server = h2.connection.H2Connection(config=self.server_config)
        server.initiate_connection()
        server.clear_outbound_data_buffer()

        server.send_origin([])

        expected = OriginFrame(stream_id=0, origins=[])
        assert server.data_to_send() == expected.serialize()

    @pytest.mark.parametrize("origin", [
        "not-an-origin",
        "https://exämple.com",
        b"https://example.com:99999",
        b"",
        42,
    ])
    def test_sending_invalid_origin(self, origin) -> None:
        """
        send_origin rejects origins that are not valid serialized origins.
        """
        server = h2.connection.H2Connection(config=self.server_config)
        server.initiate_connection()
        server.clear_outbound_data_buffer()

        with pytest.raises(ValueError):
            server.send_origin([origin])

        assert not server.data_to_send()

    def test_receiving_origin_frame_as_server(self, frame_factory) -> None:
        """
        Servers ignore ORIGIN frames they receive.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame(origins=self.example_origins)
        events = c.receive_data(f.serialize())

        assert not events
        assert c.declared_origins == frozenset()
        assert not c.data_to_send()
