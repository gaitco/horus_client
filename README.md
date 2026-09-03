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

An API-specific generated class should wrap these endpoints so application
code calls `api.showTask(7)` and never handles paths or maps directly. The
runtime deliberately contains no schema language or generator; Maat's
planned OpenAPI export will be the source of truth for generated clients.

Responses outside `200..299` throw `HorusClientException`, preserving the
status, decoded response body, and headers.
