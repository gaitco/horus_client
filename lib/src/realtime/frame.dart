import 'dart:convert';

/// One Pusher-protocol frame, in the shape Thoth actually puts on the wire.
///
/// The asymmetry worth knowing: `data` is itself a JSON **string** inside the
/// outer JSON object — Thoth's `PusherFrame.encode` calls `jsonEncode` on it
/// before encoding the envelope. So a client decodes twice, and this type is
/// the one place that happens.
final class RealtimeFrame {
  const RealtimeFrame(this.event, this.data, {this.channel});

  final String event;
  final Object? data;
  final String? channel;

  /// Null when [message] is not a frame this client can read. A malformed
  /// message is dropped rather than thrown: one bad payload from a server or
  /// a proxy is not a reason to tear down every channel on the socket.
  static RealtimeFrame? decode(Object? message) {
    if (message is! String) return null;
    final Object? envelope;
    try {
      envelope = jsonDecode(message);
    } on FormatException {
      return null;
    }
    if (envelope is! Map) return null;
    final event = envelope['event'];
    if (event is! String) return null;
    final channel = envelope['channel'];
    return RealtimeFrame(
      event,
      _inner(envelope['data']),
      channel: channel is String ? channel : null,
    );
  }

  String encode() => jsonEncode({
    'event': event,
    if (data != null) 'data': data,
    'channel': ?channel,
  });

  /// `pusher_internal:` frames are the protocol talking about itself;
  /// everything else is an application event a subscriber asked for.
  bool get isProtocol =>
      event.startsWith('pusher:') || event.startsWith('pusher_internal:');

  /// [data] as a map, or an empty map when it is anything else.
  Map<String, Object?> get dataMap =>
      data is Map ? Map<String, Object?>.from(data! as Map) : const {};

  static Object? _inner(Object? data) {
    if (data is! String) return data;
    try {
      return jsonDecode(data);
    } on FormatException {
      // A server that sends a plain string rather than encoded JSON means it
      // literally, so hand the string through instead of losing the payload.
      return data;
    }
  }
}
