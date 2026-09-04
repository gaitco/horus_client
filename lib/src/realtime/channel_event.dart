/// One typed event on one channel: the realtime twin of `Endpoint<T>`.
///
/// ```dart
/// ChannelEvent<OrderUpdated> orderUpdated(int id) => ChannelEvent(
///   channel: 'private-orders.$id',
///   event: 'OrderUpdated',
///   decode: (json) => OrderUpdated.fromJson(json! as Map<String, Object?>),
/// );
/// ```
///
/// [event] is what the server's `broadcastAs()` returns, and [channel] what
/// its `broadcastOn()` names, prefix included.
final class ChannelEvent<T> {
  const ChannelEvent({
    required this.channel,
    required this.event,
    required this.decode,
  });

  final String channel;
  final String event;
  final T Function(Object? json) decode;

  /// Private and presence channels are the ones needing a signature from the
  /// application before Thoth will let a socket subscribe.
  bool get requiresAuthorization =>
      channel.startsWith('private-') || channel.startsWith('presence-');
}
