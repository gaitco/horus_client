import 'dart:async';
import 'dart:convert';

import 'package:horus_client/realtime.dart';

/// A socket a test drives by hand: no server, no port, no waiting.
final class FakeSocket implements RealtimeSocket {
  FakeSocket(this.uri);

  final Uri uri;
  final List<Map<String, Object?>> sent = [];
  final StreamController<Object?> _incoming = StreamController<Object?>();

  bool closed = false;

  @override
  Stream<Object?> get messages => _incoming.stream;

  @override
  void send(String data) =>
      sent.add(Map<String, Object?>.from(jsonDecode(data) as Map));

  @override
  Future<void> close([int? code, String? reason]) async {
    if (closed) return;
    closed = true;
    await _incoming.close();
  }

  /// Frames the client sent, by event name.
  List<Map<String, Object?>> sentOf(String event) => [
    for (final frame in sent)
      if (frame['event'] == event) frame,
  ];

  Map<String, Object?> dataOf(Map<String, Object?> frame) =>
      Map<String, Object?>.from(frame['data']! as Map);

  /// Pushes a frame from the server, encoded exactly as Thoth encodes it —
  /// `data` is a JSON string inside the envelope.
  void emit(String event, Object? data, {String? channel}) {
    if (_incoming.isClosed) return;
    _incoming.add(
      jsonEncode({
        'event': event,
        'data': jsonEncode(data),
        'channel': ?channel,
      }),
    );
  }

  void establish([String socketId = '1234.5678']) => emit(
    'pusher:connection_established',
    {'socket_id': socketId, 'activity_timeout': 120},
  );

  void succeed(String channel) => emit(
    'pusher_internal:subscription_succeeded',
    const <String, Object?>{},
    channel: channel,
  );

  /// The server (or the network) dropping the connection.
  Future<void> drop() async {
    closed = true;
    await _incoming.close();
  }
}

/// Hands out [FakeSocket]s and remembers them, so a test can drop one and
/// assert on the next.
final class FakeTransport {
  final List<FakeSocket> sockets = [];

  FakeSocket get latest => sockets.last;
  int get connections => sockets.length;

  Future<RealtimeSocket> connect(Uri uri) async {
    final socket = FakeSocket(uri);
    sockets.add(socket);
    return socket;
  }
}

/// Lets pending microtasks and zero-length timers run.
Future<void> settle([int rounds = 6]) async {
  for (var i = 0; i < rounds; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}
