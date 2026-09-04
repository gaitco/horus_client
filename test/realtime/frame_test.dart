import 'dart:convert';

import 'package:horus_client/src/realtime/frame.dart';
import 'package:test/test.dart';

/// Exactly what Thoth's `PusherFrame.encode` puts on the wire.
String asThothEncodes(String event, Object? data, {String? channel}) =>
    jsonEncode({'event': event, 'data': jsonEncode(data), 'channel': ?channel});

void main() {
  test('decodes the double-encoded data Thoth sends', () {
    final frame = RealtimeFrame.decode(
      asThothEncodes('pusher:connection_established', {
        'socket_id': '1234.5678',
        'activity_timeout': 120,
      }),
    )!;

    expect(frame.event, 'pusher:connection_established');
    expect(frame.channel, isNull);
    // The payload survived both layers: a single decode would leave a String.
    expect(frame.dataMap['socket_id'], '1234.5678');
    expect(frame.dataMap['activity_timeout'], 120);
  });

  test('carries the channel on an application event', () {
    final frame = RealtimeFrame.decode(
      asThothEncodes('OrderUpdated', {
        'id': 7,
        'status': 'shipped',
      }, channel: 'private-orders.7'),
    )!;

    expect(frame.event, 'OrderUpdated');
    expect(frame.channel, 'private-orders.7');
    expect(frame.isProtocol, isFalse);
    expect(frame.dataMap, {'id': 7, 'status': 'shipped'});
  });

  test('protocol frames are told apart from application events', () {
    expect(
      RealtimeFrame.decode(asThothEncodes('pusher:ping', {}))!.isProtocol,
      isTrue,
    );
    expect(
      RealtimeFrame.decode(
        asThothEncodes('pusher_internal:subscription_succeeded', {}),
      )!.isProtocol,
      isTrue,
    );
    expect(
      RealtimeFrame.decode(asThothEncodes('OrderUpdated', {}))!.isProtocol,
      isFalse,
    );
  });

  test('a malformed message decodes to null rather than throwing', () {
    // Dropping one bad payload keeps every other channel on the socket alive.
    expect(RealtimeFrame.decode('not json'), isNull);
    expect(RealtimeFrame.decode('[1,2,3]'), isNull);
    expect(RealtimeFrame.decode(jsonEncode({'no': 'event'})), isNull);
    expect(RealtimeFrame.decode(const [1, 2, 3]), isNull);
  });

  test('a plain-string data field is handed through, not lost', () {
    expect(
      RealtimeFrame.decode(jsonEncode({'event': 'x', 'data': 'hello'}))!.data,
      'hello',
    );
  });

  test('encodes a subscribe frame the way Thoth parses it', () {
    final encoded = const RealtimeFrame('pusher:subscribe', {
      'channel': 'private-orders.7',
      'auth': 'key:sig',
    }).encode();

    // Thoth's `_dataMap` accepts either a String or a Map here, so the plain
    // map is what goes out.
    expect(jsonDecode(encoded), {
      'event': 'pusher:subscribe',
      'data': {'channel': 'private-orders.7', 'auth': 'key:sig'},
    });
  });
}
