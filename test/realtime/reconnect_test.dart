import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:horus_client/realtime.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

import 'fake_socket.dart';

final orderUpdated = ChannelEvent<Map<String, Object?>>(
  channel: 'private-orders.7',
  event: 'OrderUpdated',
  decode: (json) => Map<String, Object?>.from(json! as Map),
);

final otherOrder = ChannelEvent<Map<String, Object?>>(
  channel: 'private-orders.8',
  event: 'OrderUpdated',
  decode: (json) => Map<String, Object?>.from(json! as Map),
);

void main() {
  late FakeTransport transport;

  HorusRealtime build({
    HorusClient? client,
    Future<void> Function()? onAuthFailure,
    RealtimeTransport? socketTransport,
    Backoff? reconnectBackoff,
  }) => HorusRealtime(
    socketUri: Uri.parse('ws://localhost:6001/app/orders-key'),
    client: client,
    onAuthFailure: onAuthFailure,
    transport: socketTransport ?? transport.connect,
    backoff: reconnectBackoff ?? (_) => Duration.zero,
  );

  /// Signs with whatever socket id it is given, so a test can prove the
  /// signature was recomputed rather than replayed.
  HorusClient signingClient({
    List<Map<String, Object?>>? record,
    int Function(int call)? status,
  }) {
    var call = 0;
    return HorusClient(
      Uri.parse('https://api.example.test'),
      httpClient: MockClient((request) async {
        final body = Map<String, Object?>.from(jsonDecode(request.body) as Map);
        record?.add(body);
        final code = status?.call(++call) ?? 200;
        if (code != 200) {
          return http.Response(jsonEncode({'message': 'Forbidden'}), code);
        }
        return http.Response(
          jsonEncode({
            'auth': 'key:${body['socket_id']}:${body['channel_name']}',
          }),
          200,
        );
      }),
    );
  }

  setUp(() => transport = FakeTransport());

  group('reconnect', () {
    test(
      'the first connect future survives an initial transport failure',
      () async {
        var calls = 0;
        final recovered = Completer<FakeSocket>();
        final realtime = build(
          socketTransport: (uri) async {
            if (++calls == 1) throw StateError('offline');
            final socket = FakeSocket(uri);
            recovered.complete(socket);
            return socket;
          },
        );
        addTearDown(realtime.close);

        final connected = realtime.connect();
        final socket = await recovered.future;
        socket.establish('22.22');

        await expectLater(
          connected.timeout(const Duration(milliseconds: 200)),
          completes,
        );
        expect(calls, 2);
      },
    );

    test('an explicit connect cancels a pending automatic retry', () async {
      final realtime = build(
        reconnectBackoff: (_) => const Duration(milliseconds: 40),
      );
      addTearDown(realtime.close);

      unawaited(realtime.connect());
      await settle();
      transport.latest.establish('11.11');
      await settle();
      await transport.latest.drop();
      await settle();

      unawaited(realtime.connect());
      await settle();
      expect(transport.connections, 2);
      transport.latest.establish('22.22');
      await Future<void>.delayed(const Duration(milliseconds: 80));

      expect(transport.connections, 2);
    });

    test('resubscribes with a signature for the NEW socket id', () async {
      final requests = <Map<String, Object?>>[];
      final realtime = build(client: signingClient(record: requests));
      addTearDown(realtime.close);

      final received = <Map<String, Object?>>[];
      realtime.stream(orderUpdated).listen(received.add);
      await settle();
      transport.latest.establish('11.11');
      await settle();
      transport.latest.succeed('private-orders.7');
      await settle();

      await transport.latest.drop();
      await settle();

      expect(transport.connections, 2, reason: 'the client reconnected');
      transport.latest.establish('22.22');
      await settle();

      // Thoth signs a private subscription as HMAC(secret,
      // "$socketId:$channel"), so the new socket id has invalidated the old
      // signature. Replaying it would be refused.
      expect(requests, [
        {'socket_id': '11.11', 'channel_name': 'private-orders.7'},
        {'socket_id': '22.22', 'channel_name': 'private-orders.7'},
      ]);
      final subscribe = transport.latest.sentOf('pusher:subscribe').single;
      expect(
        transport.latest.dataOf(subscribe)['auth'],
        'key:22.22:private-orders.7',
      );

      transport.latest.emit('OrderUpdated', {
        'id': 7,
        'status': 'delivered',
      }, channel: 'private-orders.7');
      await settle();
      expect(received.single, {'id': 7, 'status': 'delivered'});
    });

    test('discards authorization completed for a replaced socket', () async {
      final firstResponse = Completer<http.Response>();
      final requests = <Map<String, Object?>>[];
      final client = HorusClient(
        Uri.parse('https://api.example.test'),
        httpClient: MockClient((request) async {
          final body = Map<String, Object?>.from(
            jsonDecode(request.body) as Map,
          );
          requests.add(body);
          if (requests.length == 1) return firstResponse.future;
          return http.Response(
            jsonEncode({
              'auth': 'key:${body['socket_id']}:${body['channel_name']}',
            }),
            200,
          );
        }),
      );
      final realtime = build(client: client);
      addTearDown(realtime.close);

      realtime.stream(orderUpdated).listen((_) {});
      await settle();
      transport.latest.establish('11.11');
      await settle();
      expect(requests.single['socket_id'], '11.11');

      await transport.latest.drop();
      await settle();
      transport.latest.establish('22.22');
      await settle();
      firstResponse.complete(
        http.Response(jsonEncode({'auth': 'key:11.11:private-orders.7'}), 200),
      );
      await settle(12);

      expect(requests, [
        {'socket_id': '11.11', 'channel_name': 'private-orders.7'},
        {'socket_id': '22.22', 'channel_name': 'private-orders.7'},
      ]);
      final subscribe = transport.latest.sentOf('pusher:subscribe').single;
      expect(
        transport.latest.dataOf(subscribe)['auth'],
        'key:22.22:private-orders.7',
      );
    });

    test('ignores an authorization error from a replaced socket', () async {
      final firstResponse = Completer<http.Response>();
      final requests = <Map<String, Object?>>[];
      final client = HorusClient(
        Uri.parse('https://api.example.test'),
        httpClient: MockClient((request) async {
          final body = Map<String, Object?>.from(
            jsonDecode(request.body) as Map,
          );
          requests.add(body);
          if (requests.length == 1) return firstResponse.future;
          return http.Response(
            jsonEncode({
              'auth': 'key:${body['socket_id']}:${body['channel_name']}',
            }),
            200,
          );
        }),
      );
      final realtime = build(client: client);
      addTearDown(realtime.close);
      final errors = <Object>[];

      realtime.stream(orderUpdated).listen((_) {}, onError: errors.add);
      await settle();
      transport.latest.establish('11.11');
      await settle();
      await transport.latest.drop();
      await settle();
      transport.latest.establish('22.22');
      firstResponse.complete(
        http.Response(jsonEncode({'message': 'Old socket'}), 403),
      );
      await settle(12);

      expect(errors, isEmpty);
      expect(requests, [
        {'socket_id': '11.11', 'channel_name': 'private-orders.7'},
        {'socket_id': '22.22', 'channel_name': 'private-orders.7'},
      ]);
      expect(transport.latest.sentOf('pusher:subscribe'), hasLength(1));
    });

    test('a channel whose listener left is not resubscribed', () async {
      final realtime = build(client: signingClient());
      addTearDown(realtime.close);

      final leaving = realtime.stream(orderUpdated).listen((_) {});
      realtime.stream(otherOrder).listen((_) {});
      await settle();
      transport.latest.establish('11.11');
      await settle();

      await leaving.cancel();
      await transport.latest.drop();
      await settle();
      transport.latest.establish('22.22');
      await settle();

      final channels = [
        for (final frame in transport.latest.sentOf('pusher:subscribe'))
          transport.latest.dataOf(frame)['channel'],
      ];
      expect(channels, ['private-orders.8']);
    });

    test('reports reconnecting, then connected again', () async {
      final realtime = build();
      addTearDown(realtime.close);

      final seen = <RealtimeStatus>[];
      realtime.statuses.listen(seen.add);
      unawaited(realtime.connect());
      await settle();
      transport.latest.establish();
      await settle();
      await transport.latest.drop();
      await settle();
      transport.latest.establish('22.22');
      await settle();

      expect(seen, [
        RealtimeStatus.connecting,
        RealtimeStatus.connected,
        RealtimeStatus.reconnecting,
        RealtimeStatus.connected,
      ]);
    });

    test('the socket id is null while the connection is down', () async {
      final realtime = build();
      addTearDown(realtime.close);

      unawaited(realtime.connect());
      await settle();
      transport.latest.establish('11.11');
      await settle();
      expect(realtime.socketId, '11.11');

      await transport.latest.drop();
      await settle();

      // A stale id here would tell the server to exclude a socket that no
      // longer exists, silently dropping the client's own events.
      expect(realtime.socketId, isNull);
    });

    test('a fatal refusal stops retrying instead of spinning', () async {
      final realtime = build();
      addTearDown(realtime.close);

      final errors = <Object>[];
      realtime.stream(orderUpdated).listen((_) {}, onError: errors.add);
      await settle();
      // 4001 is Thoth's "unknown application key" — retrying cannot fix it.
      transport.latest.emit('pusher:error', {
        'code': 4001,
        'message': 'Unknown application key',
      });
      await settle();
      await settle();

      expect(transport.connections, 1);
      expect(errors, hasLength(1));
      expect('${errors.single}', contains('4001'));
    });

    test('backoff grows and is capped', () {
      // Full jitter picks in [0, ceiling], so the ceiling is what grows.
      final fixed = exponentialBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 2),
        random: _MaxRandom(),
      );

      expect(fixed(1), const Duration(milliseconds: 100));
      expect(fixed(2), const Duration(milliseconds: 200));
      expect(fixed(3), const Duration(milliseconds: 400));
      expect(fixed(10), const Duration(seconds: 2));
      expect(fixed(1000), const Duration(seconds: 2));

      final jittered = exponentialBackoff(
        base: const Duration(milliseconds: 100),
        random: Random(1),
      );
      expect(jittered(5).inMilliseconds, inInclusiveRange(0, 1600));
    });
  });

  group('expired token', () {
    test('concurrent refusals share one token refresh', () async {
      var token = 'expired';
      var expiredRequests = 0;
      var refreshes = 0;
      final bothExpired = Completer<void>();
      final finishRefresh = Completer<void>();
      addTearDown(() {
        if (!finishRefresh.isCompleted) finishRefresh.complete();
      });
      final client = HorusClient(
        Uri.parse('https://api.example.test'),
        headersProvider: () async => {'authorization': 'Bearer $token'},
        httpClient: MockClient((request) async {
          if (request.headers['authorization'] == 'Bearer expired') {
            if (++expiredRequests == 2) bothExpired.complete();
            await bothExpired.future;
            return http.Response(jsonEncode({'message': 'Expired'}), 401);
          }
          final body = Map<String, Object?>.from(
            jsonDecode(request.body) as Map,
          );
          return http.Response(
            jsonEncode({
              'auth': 'key:${body['socket_id']}:${body['channel_name']}',
            }),
            200,
          );
        }),
      );
      final realtime = build(
        client: client,
        onAuthFailure: () async {
          refreshes++;
          token = 'fresh';
          await finishRefresh.future;
        },
      );
      addTearDown(realtime.close);

      realtime.stream(orderUpdated).listen((_) {});
      realtime.stream(otherOrder).listen((_) {});
      await settle();
      transport.latest.establish('11.11');
      await bothExpired.future;
      await settle();

      expect(refreshes, 1);
      finishRefresh.complete();
      await settle(12);
      expect(transport.latest.sentOf('pusher:subscribe'), hasLength(2));
    });

    test('refreshes once and completes the subscription', () async {
      var refreshed = 0;
      final requests = <Map<String, Object?>>[];
      final realtime = build(
        client: signingClient(
          record: requests,
          status: (call) => call == 1 ? 401 : 200,
        ),
        onAuthFailure: () async => refreshed++,
      );
      addTearDown(realtime.close);

      final received = <Map<String, Object?>>[];
      final errors = <Object>[];
      realtime.stream(orderUpdated).listen(received.add, onError: errors.add);
      await settle();
      transport.latest.establish('11.11');
      await settle();

      expect(refreshed, 1);
      expect(requests, hasLength(2), reason: 'the refused call was retried');
      expect(errors, isEmpty);

      final subscribe = transport.latest.sentOf('pusher:subscribe').single;
      expect(
        transport.latest.dataOf(subscribe)['auth'],
        'key:11.11:private-orders.7',
      );

      transport.latest.emit('OrderUpdated', {
        'id': 7,
        'status': 'shipped',
      }, channel: 'private-orders.7');
      await settle();
      expect(received, hasLength(1));
    });

    test('a 403 refreshes too — an expired token reads as either', () async {
      var refreshed = 0;
      final realtime = build(
        client: signingClient(status: (call) => call == 1 ? 403 : 200),
        onAuthFailure: () async => refreshed++,
      );
      addTearDown(realtime.close);

      realtime.stream(orderUpdated).listen((_) {});
      await settle();
      transport.latest.establish();
      await settle();

      expect(refreshed, 1);
      expect(transport.latest.sentOf('pusher:subscribe'), hasLength(1));
    });

    test(
      'a second refusal errors that channel and spares the others',
      () async {
        var refreshed = 0;
        final realtime = build(
          client: signingClient(
            // Channel 7 is refused twice; channel 8 is fine.
            status: (call) => call <= 2 ? 401 : 200,
          ),
          onAuthFailure: () async => refreshed++,
        );
        addTearDown(realtime.close);

        final errors = <Object>[];
        final other = <Map<String, Object?>>[];
        realtime.stream(orderUpdated).listen((_) {}, onError: errors.add);
        await settle();
        transport.latest.establish();
        await settle();
        realtime.stream(otherOrder).listen(other.add);
        await settle();

        expect(refreshed, 1, reason: 'refresh is tried once, not in a loop');
        expect(errors.single, isA<ChannelAuthorizationException>());
        expect('${errors.single}', contains('private-orders.7'));

        // An expired token for one order must not take the socket down.
        expect(transport.latest.closed, isFalse);
        transport.latest.emit('OrderUpdated', {
          'id': 8,
          'status': 'shipped',
        }, channel: 'private-orders.8');
        await settle();
        expect(other, hasLength(1));
      },
    );

    test('without a refresh hook the refusal surfaces immediately', () async {
      var calls = 0;
      final realtime = build(
        client: signingClient(
          status: (_) {
            calls++;
            return 401;
          },
        ),
      );
      addTearDown(realtime.close);

      final errors = <Object>[];
      realtime.stream(orderUpdated).listen((_) {}, onError: errors.add);
      await settle();
      transport.latest.establish();
      await settle();

      expect(calls, 1, reason: 'no hook means no retry');
      expect(errors.single, isA<ChannelAuthorizationException>());
    });
  });
}

/// Full jitter with the dice loaded, so the ceiling itself is asserted.
final class _MaxRandom implements Random {
  @override
  int nextInt(int max) => max - 1;

  @override
  bool nextBool() => true;

  @override
  double nextDouble() => 1;
}
