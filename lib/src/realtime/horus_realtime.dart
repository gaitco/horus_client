import 'dart:async';

import '../../horus_client.dart';
import 'backoff.dart';
import 'channel_event.dart';
import 'frame.dart';
import 'transport.dart';

/// Where the connection is, for a banner or a retry button.
enum RealtimeStatus { disconnected, connecting, connected, reconnecting }

/// A private or presence channel the application refused to authorize.
final class ChannelAuthorizationException implements Exception {
  const ChannelAuthorizationException(this.channel, this.cause);

  final String channel;
  final Object cause;

  @override
  String toString() =>
      'ChannelAuthorizationException($channel): not authorized — $cause';
}

/// Typed server events from Thoth, as Dart streams.
///
/// ```dart
/// final realtime = HorusRealtime(
///   socketUri: Uri.parse('ws://localhost:6001/app/orders-key'),
///   client: api,
/// );
/// final Stream<OrderUpdated> updates = realtime.stream(orderUpdated(7));
/// ```
///
/// A channel is subscribed when its first listener arrives and dropped when
/// its last one cancels, so a screen that pops stops paying for it.
final class HorusRealtime {
  HorusRealtime({
    required this.socketUri,
    this.client,
    this.authPath = '/broadcasting/auth',
    this.onAuthFailure,
    RealtimeTransport? transport,
    Backoff? backoff,
    this.protocol = '7',
  }) : _transport = transport ?? connectWebSocket,
       _backoff = backoff ?? exponentialBackoff();

  /// Thoth's endpoint for this application, `ws://host:port/app/{key}`.
  final Uri socketUri;

  /// Authorizes private channels and supplies the credentials to do it with.
  /// Only public channels work without one.
  final HorusClient? client;

  /// Maat's channel authorization endpoint.
  final String authPath;

  /// Called once when authorization is refused, before one retry — the hook
  /// for refreshing an expired token. Returning normally means "try again".
  final Future<void> Function()? onAuthFailure;

  final String protocol;

  final RealtimeTransport _transport;
  final Backoff _backoff;

  final Map<String, _Channel> _channels = {};
  final StreamController<RealtimeStatus> _status =
      StreamController<RealtimeStatus>.broadcast();

  RealtimeSocket? _socket;
  StreamSubscription<Object?>? _messages;
  String? _socketId;
  int _attempt = 0;
  bool _closed = false;
  bool _connecting = false;
  int _generation = 0;
  Timer? _retry;
  Timer? _activity;
  Timer? _pong;
  Duration _activityTimeout = const Duration(seconds: 120);
  final Duration _pongTimeout = const Duration(seconds: 30);
  RealtimeStatus _current = RealtimeStatus.disconnected;
  Completer<void>? _ready;
  Future<void>? _authRefresh;

  /// The connection's id, or null while it is down.
  ///
  /// Hand this to [HorusClient.socketIdProvider] and the server will skip
  /// this client when a handler broadcasts with `.toOthers(request)`.
  String? get socketId => _socketId;

  RealtimeStatus get status => _current;

  Stream<RealtimeStatus> get statuses => _status.stream;

  /// Opens the connection and completes once the server has assigned a socket
  /// id. Called for you by the first [stream] listener.
  Future<void> connect() {
    if (_closed) throw StateError('This HorusRealtime has been closed.');
    if (_socketId != null) return Future.value();
    _retry?.cancel();
    _retry = null;
    return _open();
  }

  /// The typed events matching [event]. Broadcast, so several widgets may
  /// listen to one channel.
  Stream<T> stream<T>(ChannelEvent<T> event) {
    final channel = _channels.putIfAbsent(
      event.channel,
      () => _Channel(event.channel),
    );
    late final StreamController<T> controller;
    StreamSubscription<RealtimeFrame>? frames;
    controller = StreamController<T>.broadcast(
      onListen: () {
        channel.listeners++;
        frames = channel.frames.stream.listen((frame) {
          if (frame.event != event.event) return;
          try {
            controller.add(event.decode(frame.data));
          } catch (error, stackTrace) {
            // A payload this listener cannot read is this listener's problem.
            // Every other channel on the socket keeps running.
            controller.addError(error, stackTrace);
          }
        }, onDone: () => unawaited(controller.close()));
        channel.errors.add(controller);
        unawaited(_activate(channel));
      },
      onCancel: () async {
        await frames?.cancel();
        frames = null;
        channel.errors.remove(controller);
        if (--channel.listeners > 0) return;
        await _deactivate(channel);
      },
    );
    return controller.stream;
  }

  Future<void> close() async {
    _closed = true;
    _retry?.cancel();
    _cancelHeartbeat();
    await _teardown();
    for (final channel in _channels.values) {
      await channel.frames.close();
    }
    _channels.clear();
    await _status.close();
  }

  // --- connection ---------------------------------------------------------

  Future<void> _open() {
    final currentReady = _ready;
    final ready = currentReady == null || currentReady.isCompleted
        ? (_ready = Completer<void>())
        : currentReady;
    if (_connecting) return ready.future;
    _connecting = true;
    final generation = ++_generation;
    _emit(
      _attempt == 0 ? RealtimeStatus.connecting : RealtimeStatus.reconnecting,
    );
    unawaited(() async {
      try {
        final socket = await _transport(_uri());
        if (_closed || generation != _generation) {
          await socket.close();
          return;
        }
        _socket = socket;
        _messages = socket.messages.listen(
          (message) => _onMessage(generation, message),
          onError: (Object error, StackTrace stackTrace) => _onDone(generation),
          onDone: () => _onDone(generation),
          cancelOnError: true,
        );
      } catch (error) {
        if (generation != _generation) return;
        _connecting = false;
        _scheduleReconnect();
      }
    }());
    return ready.future;
  }

  Uri _uri() {
    final query = {
      ...socketUri.queryParameters,
      'protocol': protocol,
      'client': 'horus',
      'version': '0.1.0',
    };
    return socketUri.replace(queryParameters: query);
  }

  void _onMessage(int generation, Object? message) {
    if (generation != _generation) return;
    _resetHeartbeat();
    final frame = RealtimeFrame.decode(message);
    if (frame == null) return;
    switch (frame.event) {
      case 'pusher:connection_established':
        _onEstablished(frame);
      case 'pusher:ping':
        _send(const RealtimeFrame('pusher:pong', <String, Object?>{}));
      case 'pusher:pong':
        _pong?.cancel();
      case 'pusher:error':
        _onError(frame);
      case 'pusher_internal:subscription_succeeded':
        final channel = _channels[frame.channel];
        if (channel != null) {
          channel.subscriptionRequested = false;
          channel.subscribed = channel.listeners > 0;
        }
      default:
        if (frame.isProtocol) return;
        final channel = _channels[frame.channel];
        if (channel != null && !channel.frames.isClosed) {
          channel.frames.add(frame);
        }
    }
  }

  void _onEstablished(RealtimeFrame frame) {
    final data = frame.dataMap;
    final id = data['socket_id'];
    if (id is! String) return;
    _socketId = id;
    _attempt = 0;
    _connecting = false;
    final timeout = data['activity_timeout'];
    if (timeout is int && timeout > 0) {
      _activityTimeout = Duration(seconds: timeout);
    }
    _emit(RealtimeStatus.connected);
    _resetHeartbeat();
    _ready?.complete();
    // Every live channel is resubscribed from scratch. A private channel's
    // signature is HMAC(secret, "$socketId:$channel"), so the new socket id
    // has invalidated every signature this client was holding — replaying the
    // old subscribe frames would be refused.
    for (final channel in _channels.values) {
      channel.subscribed = false;
      channel.subscriptionRequested = false;
      if (channel.listeners > 0) unawaited(_activate(channel));
    }
  }

  void _onError(RealtimeFrame frame) {
    final data = frame.dataMap;
    final code = data['code'];
    final message = data['message'] ?? 'Realtime error';
    // A wrong key or unsupported protocol cannot fix itself by retrying;
    // anything else might.
    if (code is int && code >= 4000 && code < 4100) {
      _fail(StateError('Thoth refused the connection ($code): $message'));
      return;
    }
  }

  void _onDone(int generation) {
    if (generation != _generation) return;
    _generation++;
    _connecting = false;
    _messages = null;
    _socket = null;
    _socketId = null;
    _cancelHeartbeat();
    for (final channel in _channels.values) {
      channel.subscribed = false;
      channel.subscriptionRequested = false;
    }
    if (_closed) return;
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    if (_closed || _retry != null) return;
    _emit(RealtimeStatus.reconnecting);
    _attempt++;
    _retry = Timer(_backoff(_attempt), () {
      _retry = null;
      if (_closed) return;
      unawaited(_open().catchError((Object _) {}));
    });
  }

  /// A refusal the connection cannot recover from: stop retrying and tell
  /// every listener, rather than spinning silently forever.
  void _fail(Object error) {
    _closed = true;
    _retry?.cancel();
    _cancelHeartbeat();
    _emit(RealtimeStatus.disconnected);
    final ready = _ready;
    if (ready != null && !ready.isCompleted) ready.completeError(error);
    for (final channel in _channels.values) {
      channel.addError(error);
    }
    unawaited(_teardown());
  }

  Future<void> _teardown() async {
    final messages = _messages;
    final socket = _socket;
    _messages = null;
    _socket = null;
    _socketId = null;
    await messages?.cancel();
    await socket?.close();
  }

  // --- heartbeat ----------------------------------------------------------

  void _resetHeartbeat() {
    _activity?.cancel();
    _pong?.cancel();
    if (_closed) return;
    _activity = Timer(_activityTimeout, () {
      _send(const RealtimeFrame('pusher:ping', <String, Object?>{}));
      _pong = Timer(_pongTimeout, () {
        // A silent socket is worse than a closed one: it looks connected
        // while delivering nothing. Drop it and let reconnect run.
        final generation = _generation;
        unawaited(_teardown().then((_) => _onDone(generation)));
      });
    });
  }

  void _cancelHeartbeat() {
    _activity?.cancel();
    _pong?.cancel();
    _activity = null;
    _pong = null;
  }

  // --- subscriptions ------------------------------------------------------

  Future<void> _activate(_Channel channel) async {
    if (_closed) return;
    if (_socketId == null) {
      if (!_connecting && _retry == null) {
        unawaited(_open().catchError((Object _) {}));
      }
      return;
    }
    if (channel.subscribed ||
        channel.subscribing ||
        channel.subscriptionRequested) {
      return;
    }
    final socketId = _socketId!;
    var retryForNewSocket = false;
    channel.subscribing = true;
    try {
      final auth = await _authorize(channel.name, socketId);
      // The socket may have dropped while the authorization was in flight,
      // which means the signature just obtained is already for a dead id.
      if (_closed || channel.listeners == 0) return;
      if (_socketId != socketId) {
        retryForNewSocket = _socketId != null;
        return;
      }
      channel.subscriptionRequested = true;
      _send(
        RealtimeFrame('pusher:subscribe', {
          'channel': channel.name,
          'auth': ?auth?.auth,
          'channel_data': ?auth?.channelData,
        }),
      );
    } catch (error) {
      if (!_closed && channel.listeners > 0 && _socketId != socketId) {
        retryForNewSocket = _socketId != null;
      } else {
        channel.addError(ChannelAuthorizationException(channel.name, error));
      }
    } finally {
      channel.subscribing = false;
      if (retryForNewSocket) unawaited(_activate(channel));
    }
  }

  Future<void> _deactivate(_Channel channel) async {
    if ((!channel.subscribed && !channel.subscriptionRequested) ||
        _socketId == null) {
      return;
    }
    channel.subscribed = false;
    channel.subscriptionRequested = false;
    _send(RealtimeFrame('pusher:unsubscribe', {'channel': channel.name}));
  }

  /// Null for a public channel; a signature from the application otherwise.
  Future<_Authorization?> _authorize(String channel, String socketId) async {
    if (!channel.startsWith('private-') && !channel.startsWith('presence-')) {
      return null;
    }
    final http = client;
    if (http == null) {
      throw StateError(
        'HorusRealtime needs a HorusClient to authorize [$channel]. Public '
        'channels work without one; private and presence channels do not.',
      );
    }
    try {
      return await _requestAuthorization(http, channel, socketId);
    } on HorusClientException catch (error) {
      final refresh = onAuthFailure;
      if (refresh == null ||
          (error.statusCode != 401 && error.statusCode != 403)) {
        rethrow;
      }
      // One retry, after giving the application a chance to refresh. A
      // second refusal is a real refusal, not an expired token.
      await _refreshAuthorization(refresh);
      return _requestAuthorization(http, channel, socketId);
    }
  }

  Future<void> _refreshAuthorization(Future<void> Function() refresh) {
    final active = _authRefresh;
    if (active != null) return active;
    final pending = Future<void>.sync(refresh);
    _authRefresh = pending;
    return pending.whenComplete(() {
      if (identical(_authRefresh, pending)) _authRefresh = null;
    });
  }

  Future<_Authorization> _requestAuthorization(
    HorusClient http,
    String channel,
    String socketId,
  ) async {
    final response = await http.send(
      Endpoint<Map<String, Object?>>(
        method: 'POST',
        path: authPath,
        decode: (json) => Map<String, Object?>.from(json! as Map),
      ),
      body: {'socket_id': socketId, 'channel_name': channel},
    );
    final auth = response['auth'];
    if (auth is! String) {
      throw StateError('$authPath answered without an "auth" signature.');
    }
    final channelData = response['channel_data'];
    return _Authorization(auth, channelData is String ? channelData : null);
  }

  void _send(RealtimeFrame frame) => _socket?.send(frame.encode());

  void _emit(RealtimeStatus status) {
    if (_current == status) return;
    _current = status;
    if (!_status.isClosed) _status.add(status);
  }
}

final class _Authorization {
  const _Authorization(this.auth, this.channelData);

  final String auth;
  final String? channelData;
}

class _Channel {
  _Channel(this.name);

  final String name;
  final StreamController<RealtimeFrame> frames =
      StreamController<RealtimeFrame>.broadcast();

  /// The controllers handed to callers, so a channel-level failure reaches
  /// the listeners rather than disappearing.
  final List<StreamController<Object?>> errors = [];

  int listeners = 0;
  bool subscribed = false;
  bool subscribing = false;
  bool subscriptionRequested = false;

  void addError(Object error) {
    for (final controller in errors) {
      if (!controller.isClosed) controller.addError(error);
    }
  }
}
