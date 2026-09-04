# Horus Client

<p align="center"><img src="assets/icon.svg" width="96" alt="Horus icon"></p>

The small, cross-platform HTTP runtime for typed Maat API clients. It has
no Flutter dependency and works on mobile, desktop, and the web.

```dart
final showTaskEndpoint = Endpoint<TaskDto>(
  method: 'GET',
  path: '/tasks/{id}',
  decode: (json) => TaskDto.fromJson(json as Map<String, Object?>),
);

final client = HorusClient(Uri.parse('https://api.example.com'));
final task = await client.send(
  showTaskEndpoint,
  pathParameters: {'id': 7},
);
```

An API-specific generated class wraps these endpoints so application code
calls `api.booksShow(id: 7)` and never handles paths or maps directly. The
runtime deliberately contains no schema language or generator: Maat writes
that class from its route contracts with `sesh api:client`. See
[OpenAPI & Typed Clients](https://github.com/gaitco/maat-monorepo/blob/main/docs/openapi.md).

Responses outside `200..299` throw `HorusClientException`, preserving the
status, decoded response body, and headers.

## Realtime

`package:horus_client/realtime.dart` is a second entrypoint carrying a
Pusher-protocol client for [Thoth](https://pub.dev/packages/thoth_realtime), so
an application that only makes requests never imports the socket code.

```dart
final realtime = HorusRealtime(
  socketUri: Uri.parse('ws://localhost:6001/app/$appKey'),
  client: client,
  onAuthFailure: tokens.refresh,
);
client.socketIdProvider = () => realtime.socketId;

final Stream<OrderUpdated> updates = realtime.stream(orderUpdated(7));
```

Private channels are authorized through the application's
`/broadcasting/auth`, reconnects re-sign every subscription against the new
socket id, and `X-Socket-ID` keeps a client from hearing its own writes.
