"""
Test the RFC 8336 ORIGIN frame support.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions


class TestRFC8336Client:
    """
    Tests that the client supports receiving the RFC 8336 ORIGIN frame.
    """

    server_config = h2.config.H2Configuration(client_side=False)

    def test_receiving_origin_frame_single_origin(self, frame_factory) -> None:
        """
        An ORIGIN frame with a single origin fires an OriginReceived event
        and populates the connection's origin set.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame([b"https://example.com"])
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == [b"https://example.com"]
        assert c.origin_set == frozenset({b"https://example.com"})

        # No data gets sent.
        assert not c.data_to_send()

    def test_receiving_origin_frame_multiple_origins(self, frame_factory) -> None:
        """
        An ORIGIN frame can declare multiple origins at once.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        origins = [
            b"https://example.com",
            b"https://www.example.com",
            b"https://example.com:8443",
            b"http://[::1]",
        ]
        f = frame_factory.build_origin_frame(origins)
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == origins
        assert c.origin_set == frozenset(origins)

    def test_origin_set_accumulates_across_frames(self, frame_factory) -> None:
        """
        Origins declared by successive ORIGIN frames accumulate in the
        connection's origin set.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f1 = frame_factory.build_origin_frame([b"https://example.com"])
        f2 = frame_factory.build_origin_frame([b"https://www.example.com"])
        events = c.receive_data(f1.serialize() + f2.serialize())

        assert len(events) == 2
        assert all(isinstance(e, h2.events.OriginReceived) for e in events)
        assert events[0].origins == [b"https://example.com"]
        assert events[1].origins == [b"https://www.example.com"]
        assert c.origin_set == frozenset(
            {b"https://example.com", b"https://www.example.com"},
        )

    def test_duplicate_origins_are_not_new_declarations(self, frame_factory) -> None:
        """
        Re-declaring an origin that is already in the origin set does not
        change the set, either within a single frame or across frames.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f1 = frame_factory.build_origin_frame(
            [b"https://example.com", b"https://example.com"],
        )
        f2 = frame_factory.build_origin_frame(
            [b"https://example.com", b"https://www.example.com"],
        )
        events = c.receive_data(f1.serialize() + f2.serialize())

        assert len(events) == 2
        # The events faithfully report the frame contents...
        assert events[0].origins == [b"https://example.com", b"https://example.com"]
        assert events[1].origins == [b"https://example.com", b"https://www.example.com"]
        # ...but the set only ever contains each origin once.
        assert c.origin_set == frozenset(
            {b"https://example.com", b"https://www.example.com"},
        )

    def test_origin_set_is_a_snapshot(self, frame_factory) -> None:
        """
        The origin_set property returns an immutable snapshot: later ORIGIN
        frames do not mutate previously returned sets.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        assert c.origin_set == frozenset()

        f1 = frame_factory.build_origin_frame([b"https://example.com"])
        c.receive_data(f1.serialize())
        first_view = c.origin_set

        f2 = frame_factory.build_origin_frame([b"https://www.example.com"])
        c.receive_data(f2.serialize())

        assert first_view == frozenset({b"https://example.com"})
        assert c.origin_set == frozenset(
            {b"https://example.com", b"https://www.example.com"},
        )

    def test_receiving_empty_origin_frame(self, frame_factory) -> None:
        """
        An ORIGIN frame with an empty payload is legal: it declares no
        origins and leaves the origin set unchanged.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame([])
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == []
        assert c.origin_set == frozenset()
        assert not c.data_to_send()

    def test_receiving_origin_frame_truncated_origin_length(self, frame_factory) -> None:
        """
        An ORIGIN frame whose payload ends partway through an Origin-Len
        field is a connection error of type PROTOCOL_ERROR.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        # A complete ORIGIN frame, then one whose payload is a single byte:
        # too short to even contain an origin length.
        good_frame = frame_factory.build_origin_frame([b"https://example.com"])
        bad_frame = (
            b"\x00\x00\x01"  # Length: 1
            b"\x0c"          # Type: ORIGIN
            b"\x00"          # Flags: none
            b"\x00\x00\x00\x00"  # Stream ID: 0
            b"\x00"          # Truncated Origin-Len
        )

        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(good_frame.serialize() + bad_frame)

        expected_frame = frame_factory.build_goaway_frame(
            last_stream_id=0, error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.data_to_send() == expected_frame.serialize()

    def test_receiving_origin_frame_truncated_origin(self, frame_factory) -> None:
        """
        An ORIGIN frame whose Origin-Len overruns the remaining payload is a
        connection error of type PROTOCOL_ERROR.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        # Origin-Len claims 18 bytes, but only 3 are present.
        bad_frame = (
            b"\x00\x00\x05"  # Length: 5
            b"\x0c"          # Type: ORIGIN
            b"\x00"          # Flags: none
            b"\x00\x00\x00\x00"  # Stream ID: 0
            b"\x00\x12abc"   # Origin-Len: 18, Origin: truncated
        )

        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(bad_frame)

        expected_frame = frame_factory.build_goaway_frame(
            last_stream_id=0, error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.data_to_send() == expected_frame.serialize()

    @pytest.mark.parametrize("bad_origin", [
        b"",                            # empty
        b"example.com",                 # no scheme
        b"://example.com",              # empty scheme
        b"https://",                    # empty host
        b"https://example.com/",        # trailing path
        b"https://example.com/path",    # path component
        b"https://example.com?",        # query delimiter
        b"https://user@example.com",    # userinfo
        b"https://exa mple.com",        # whitespace in host
        b"https://example.com:",        # empty port
        b"https://example.com:https",   # non-numeric port
        b"https://example.com:65536",   # port out of range
        b"https://example.com:99999999999999999999",  # absurd port
        b"1https://example.com",        # scheme must start with ALPHA
        b"https://example.com\x80",     # non-ASCII byte
        "https://exämple.com".encode(),  # non-ASCII host
    ])
    def test_invalid_origins_are_ignored(self, frame_factory, bad_origin) -> None:
        """
        Origins that are not valid ASCII serializations of an origin are
        ignored, per RFC 8336: they do not appear in the event or the origin
        set, and do not cause an error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame(
            [b"https://example.com", bad_origin],
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == [b"https://example.com"]
        assert c.origin_set == frozenset({b"https://example.com"})
        assert not c.data_to_send()

    def test_receiving_origin_frame_on_stream(self, frame_factory) -> None:
        """
        An ORIGIN frame received on any stream other than stream 0 is a
        connection error of type PROTOCOL_ERROR.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame(
            [b"https://example.com"], stream_id=1,
        )

        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(f.serialize())

        expected_frame = frame_factory.build_goaway_frame(
            last_stream_id=0, error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.data_to_send() == expected_frame.serialize()
        assert c.origin_set == frozenset()

    def test_server_receiving_origin_frame(self, frame_factory) -> None:
        """
        Clients MUST NOT send ORIGIN frames: servers treat receipt of one as
        a connection error of type PROTOCOL_ERROR.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame([b"https://example.com"])

        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(f.serialize())

        expected_frame = frame_factory.build_goaway_frame(
            last_stream_id=0, error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.data_to_send() == expected_frame.serialize()
        assert c.origin_set == frozenset()

    def test_origin_frame_flags_are_ignored(self, frame_factory) -> None:
        """
        The ORIGIN frame defines no flags: receivers must ignore any that
        are set.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        f = frame_factory.build_origin_frame([b"https://example.com"])
        data = bytearray(f.serialize())
        data[4] = 0xFF  # Set every flag bit.

        events = c.receive_data(bytes(data))

        assert len(events) == 1
        assert isinstance(events[0], h2.events.OriginReceived)
        assert events[0].origins == [b"https://example.com"]

    @pytest.mark.parametrize("chunk_size", list(range(1, 64)))
    def test_origin_frame_arbitrary_chunking(self, frame_factory, chunk_size) -> None:
        """
        No matter how the network chunks the data, the same events are
        emitted and the same origin set is built.
        """
        frames = [
            frame_factory.build_origin_frame(
                [b"https://example.com", b"https://www.example.com"],
            ),
            frame_factory.build_origin_frame([]),
            frame_factory.build_origin_frame(
                [b"https://example.com", b"https://api.example.com:8443"],
            ),
        ]
        data = b"".join(f.serialize() for f in frames)

        expected_events = h2.connection.H2Connection()
        expected_events.initiate_connection()
        baseline = expected_events.receive_data(data)

        c = h2.connection.H2Connection()
        c.initiate_connection()
        events = []
        for i in range(0, len(data), chunk_size):
            events.extend(c.receive_data(data[i:i + chunk_size]))

        assert events == baseline
        assert len(events) == 3
        assert all(isinstance(e, h2.events.OriginReceived) for e in events)
        assert c.origin_set == frozenset({
            b"https://example.com",
            b"https://www.example.com",
            b"https://api.example.com:8443",
        })


class TestRFC8336Server:
    """
    Tests that the server supports sending the RFC 8336 ORIGIN frame.
    """

    server_config = h2.config.H2Configuration(client_side=False)
    client_config = h2.config.H2Configuration(client_side=True)

    def test_sending_origin_frame(self, frame_factory) -> None:
        """
        Servers can send ORIGIN frames on stream 0.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        origins = [b"https://example.com", b"https://www.example.com"]
        c.send_origin_frame(origins)

        expected_frame = frame_factory.build_origin_frame(origins)
        assert c.data_to_send() == expected_frame.serialize()

    def test_sending_origin_frame_roundtrip(self, frame_factory) -> None:
        """
        An ORIGIN frame sent by a server is received by the client, firing
        an event and updating the client's origin set.
        """
        server = h2.connection.H2Connection(config=self.server_config)
        client = h2.connection.H2Connection(config=self.client_config)
        server.initiate_connection()
        client.initiate_connection()
        client.receive_data(server.data_to_send())
        server.receive_data(client.data_to_send())
        client.receive_data(server.data_to_send())

        origins = [b"https://example.com", b"https://www.example.com"]
        server.send_origin_frame(origins)
        events = client.receive_data(server.data_to_send())

        assert len(events) == 1
        event = events[0]

        assert isinstance(event, h2.events.OriginReceived)
        assert event.origins == origins
        assert client.origin_set == frozenset(origins)

        # The server's own origin set is untouched: it tracks origins
        # declared by the *remote* peer.
        assert server.origin_set == frozenset()

    def test_client_cannot_send_origin_frame(self) -> None:
        """
        Clients MUST NOT send ORIGIN frames.
        """
        c = h2.connection.H2Connection(config=self.client_config)
        c.initiate_connection()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_origin_frame([b"https://example.com"])

    @pytest.mark.parametrize("bad_origin", [
        b"",
        b"example.com",
        b"https://",
        b"https://example.com/path",
        b"https://example.com:65536",
        "https://exämple.com".encode(),
    ])
    def test_sending_invalid_origin_rejected(self, bad_origin) -> None:
        """
        Servers cannot send ORIGIN frames containing invalid origins.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        with pytest.raises(ValueError, match="not a valid ASCII origin"):
            c.send_origin_frame([b"https://example.com", bad_origin])

        assert not c.data_to_send()

    def test_sending_non_bytes_origin_rejected(self) -> None:
        """
        Origins must be bytestrings.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()

        with pytest.raises(ValueError, match="must be bytestring"):
            c.send_origin_frame(["https://example.com"])

    def test_sending_overlong_origin_rejected(self) -> None:
        """
        Origins must fit in the 16-bit Origin-Len field.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()

        with pytest.raises(ValueError, match="too long"):
            c.send_origin_frame([b"https://" + b"a" * 65536])
