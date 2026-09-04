## 0.1.1

- Add `package:horus_client/realtime.dart`, a second entrypoint carrying the Thoth realtime client, so HTTP-only applications never import the socket code.
- Subscribe to channels as typed `ChannelEvent` streams, with private-channel authorization and reconnect backoff.

## 0.1.0

- Initial release of the typed HTTP client for Maat APIs.
- Supports Dart applications across the platforms provided by package:http.
