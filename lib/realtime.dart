/// Typed server events from Thoth, as Dart streams.
///
/// A second entrypoint so an application that only makes HTTP requests never
/// imports the socket code: `package:horus_client/horus_client.dart` stays an
/// HTTP runtime.
library;

export 'horus_client.dart';
export 'src/realtime/backoff.dart';
export 'src/realtime/channel_event.dart';
export 'src/realtime/frame.dart';
export 'src/realtime/horus_realtime.dart';
export 'src/realtime/transport.dart';
