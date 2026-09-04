import 'dart:async';
import 'dart:convert';

import 'package:horus_client/realtime.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

import 'fake_socket.dart';

final class OrderUpdated {
  const OrderUpdated(this.id, this.status);

  factory OrderUpdated.fromJson(Map<String, Object?> json) =>
      OrderUpdated(json['id']! as int, json['status']! as String);

  final int id;
  final String status;
}

ChannelEvent<OrderUpdated> orderUpdated(int id) => ChannelEvent(
  channel: 'private-orders.$id',
  event: 'OrderUpdated',
  decode: (json) => OrderUpdated.fromJson(json! as Map<String, Object?>),
);

final publicPing = ChannelEvent<String>(
  channel: 'status',
  event: 'Ping',
  decode: (json) => (json! as Map<String, Object?>)['message']! as String,
);

/// A `/broadcasting/auth` that signs whatever it is asked to sign.
HorusClient authorizingClient({
  List<Map<String, Object?>>? record,
  int Function()? status,
}) => HorusClient(
  Uri.parse('https://api.example.test'),
  httpClient: MockClient((request) async {
    final body = Map<String, Object?>.from(jsonDecode(request.body) as Map);
    record?.add(body);
    final code = status?.call() ?? 200;
    if (code != 200) {
      return http.Response(jsonEncode({'message': 'Forbidden'}), code);
    }
    return http.Response(
      jsonEncode({'auth': 'key:${body['socket_id']}:${body['channel_name']}'}),
      200,
    );
  }),
);

void main() {
  late FakeTransport transport;

  HorusRealtime build({
    HorusClient? client,
    Future<void> Function()? onAuthFailure,
  }) => HorusRealtime(
    socketUri: Uri.parse('ws://localhost:6001/app/orders-key'),
    client: client,
    onAuthFailure: onAuthFailure,
    transport: transport.connect,
    backoff: (_) => Duration.zero,
  );

  setUp(() => transport = FakeTransport());

  test('connects with the protocol query Thoth requires', () async {
    final realtime = build();
    addTearDown(realtime.close);

    unawaited(realtime.connect());
    await settle();

    // Thoth rejects a connection with no protocol (4008) or the wrong one
    // (4007) before it ever reaches a channel.
    expect(transport.latest.uri.queryParameters['protocol'], '7');
    expect(transport.latest.uri.path, '/app/orders-key');
  });

  test('delivers a decoded event to a typed stream', () async {
    final realtime = build(client: authorizingClient());
    addTearDown(realtime.close);

    final received = <OrderUpdated>[];
    realtime.stream(orderUpdated(7)).listen(received.add);
    await settle();
    transport.latest.establish();
    await settle();
    transport.latest
      ..succeed('private-orders.7')
      ..emit('OrderUpdated', {
        'id': 7,
        'status': 'shipped',
      }, channel: 'private-orders.7');
    await settle();

    expect(received, hasLength(1));
    expect(received.single.id, 7);
    expect(received.single.status, 'shipped');
  });

  test('ignores another event name on the same channel', () async {
    final realtime = build(client: authorizingClient());
    addTearDown(realtime.close);

    final received = <OrderUpdated>[];
    realtime.stream(orderUpdated(7)).listen(received.add);
    await settle();
    transport.latest.establish();
    await settle();
    transport.latest.emit('OrderCancelled', {
      'id': 7,
      'status': 'cancelled',
    }, channel: 'private-orders.7');
    await settle();

    expect(received, isEmpty);
  });

  test('signs a private channel with the current socket id', () async {
    final requests = <Map<String, Object?>>[];
    final realtime = build(client: authorizingClient(record: requests));
    addTearDown(realtime.close);

    realtime.stream(orderUpdated(7)).listen((_) {});
    await settle();
    transport.latest.establish('99.11');
    await settle();

    expect(requests, [
      {'socket_id': '99.11', 'channel_name': 'private-orders.7'},
    ]);
    final subscribe = transport.latest.sentOf('pusher:subscribe').single;
    expect(transport.latest.dataOf(subscribe), {
      'channel': 'private-orders.7',
      'auth': 'key:99.11:private-orders.7',
    });
  });

  test(
    'a public channel subscribes without an authorization round trip',
    () async {
      final requests = <Map<String, Object?>>[];
      final realtime = build(client: authorizingClient(record: requests));
      addTearDown(realtime.close);

      realtime.stream(publicPing).listen((_) {});
      await settle();
      transport.latest.establish();
      await settle();

      expect(requests, isEmpty);
      final subscribe = transport.latest.sentOf('pusher:subscribe').single;
      expect(transport.latest.dataOf(subscribe), {'channel': 'status'});
    },
  );

  test(
    'two listeners on one channel subscribe once and unsubscribe last',
    () async {
      final realtime = build(client: authorizingClient());
      addTearDown(realtime.close);

      final first = realtime.stream(orderUpdated(7)).listen((_) {});
      await settle();
      transport.latest.establish();
      await settle();
      final second = realtime.stream(orderUpdated(7)).listen((_) {});
      await settle();

      // A second widget can arrive after subscribe was sent but before the
      // server acknowledges it. That is still one channel subscription.
      expect(transport.latest.sentOf('pusher:subscribe'), hasLength(1));
      transport.latest.succeed('private-orders.7');
      await settle();

      await first.cancel();
      await settle();
      // One widget closing must not cut the other one off.
      expect(transport.latest.sentOf('pusher:unsubscribe'), isEmpty);

      await second.cancel();
      await settle();
      expect(transport.latest.sentOf('pusher:unsubscribe'), hasLength(1));
    },
  );

  test(
    'cancelling before the subscribe acknowledgement unsubscribes',
    () async {
      final realtime = build();
      addTearDown(realtime.close);

      final listener = realtime.stream(publicPing).listen((_) {});
      await settle();
      transport.latest.establish();
      await settle();
      expect(transport.latest.sentOf('pusher:subscribe'), hasLength(1));

      await listener.cancel();
      await settle();

      expect(transport.latest.sentOf('pusher:unsubscribe'), hasLength(1));
    },
  );

  test('closing realtime closes typed event streams', () async {
    final realtime = build();
    final done = expectLater(
      realtime.stream(publicPing).timeout(const Duration(milliseconds: 200)),
      emitsDone,
    );
    await settle();

    await realtime.close();

    await done;
  });

  test('a decoder that throws errors one stream and leaves the rest', () async {
    final realtime = build(client: authorizingClient());
    addTearDown(realtime.close);

    final broken = ChannelEvent<int>(
      channel: 'status',
      event: 'Ping',
      decode: (json) => (json! as Map<String, Object?>)['missing']! as int,
    );
    final errors = <Object>[];
    final messages = <String>[];
    realtime.stream(broken).listen((_) {}, onError: errors.add);
    realtime.stream(publicPing).listen(messages.add);
    await settle();
    transport.latest.establish();
    await settle();
    transport.latest.emit('Ping', {'message': 'hello'}, channel: 'status');
    await settle();

    expect(errors, hasLength(1));
    // The socket is untouched, and the listener that can read the payload
    // still gets it.
    expect(messages, ['hello']);
    expect(transport.latest.closed, isFalse);
  });

  test('answers a server ping so the connection is not culled', () async {
    final realtime = build();
    addTearDown(realtime.close);

    unawaited(realtime.connect());
    await settle();
    transport.latest
      ..establish()
      ..emit('pusher:ping', const <String, Object?>{});
    await settle();

    expect(transport.latest.sentOf('pusher:pong'), hasLength(1));
  });

  test('reports its status as the connection comes up', () async {
    final realtime = build();
    addTearDown(realtime.close);

    final seen = <RealtimeStatus>[];
    realtime.statuses.listen(seen.add);
    unawaited(realtime.connect());
    await settle();
    transport.latest.establish();
    await settle();

    expect(seen, [RealtimeStatus.connecting, RealtimeStatus.connected]);
    expect(realtime.socketId, '1234.5678');
  });

  test('exposes the socket id a HorusClient sends as X-Socket-ID', () async {
    final realtime = build();
    addTearDown(realtime.close);
    final api = HorusClient(
      Uri.parse('https://api.example.test'),
      socketIdProvider: () => realtime.socketId,
    );

    expect(await api.resolveHeaders(), isNot(contains('x-socket-id')));

    unawaited(realtime.connect());
    await settle();
    transport.latest.establish('55.66');
    await settle();

    // This is what stops the server echoing a client's own write back at it.
    expect((await api.resolveHeaders())['x-socket-id'], '55.66');
  });

  test('a private channel without a client fails that stream only', () async {
    final realtime = build();
    addTearDown(realtime.close);

    final errors = <Object>[];
    realtime.stream(orderUpdated(7)).listen((_) {}, onError: errors.add);
    await settle();
    transport.latest.establish();
    await settle();

    expect(errors.single, isA<ChannelAuthorizationException>());
    expect(transport.latest.closed, isFalse);
  });
}
