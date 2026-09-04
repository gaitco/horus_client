import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

/// The socket [HorusRealtime] talks through.
///
/// An interface rather than a `WebSocketChannel` directly so a test can drop
/// a connection, replay frames and assert on what was sent without a real
/// server, a real port, or a real wait.
abstract interface class RealtimeSocket {
  /// Closes when the connection ends, however it ended.
  Stream<Object?> get messages;

  void send(String data);

  Future<void> close([int? code, String? reason]);
}

typedef RealtimeTransport = Future<RealtimeSocket> Function(Uri uri);

/// The default transport: `package:web_socket_channel`, which picks the
/// browser or the IO implementation, so the same code runs in Flutter web,
/// mobile and desktop.
Future<RealtimeSocket> connectWebSocket(Uri uri) async {
  final channel = WebSocketChannel.connect(uri);
  await channel.ready;
  return _ChannelSocket(channel);
}

final class _ChannelSocket implements RealtimeSocket {
  _ChannelSocket(this._channel);

  final WebSocketChannel _channel;

  @override
  Stream<Object?> get messages => _channel.stream;

  @override
  void send(String data) => _channel.sink.add(data);

  @override
  Future<void> close([int? code, String? reason]) =>
      _channel.sink.close(code, reason);
}
