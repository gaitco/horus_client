import 'dart:convert';

import 'package:horus_client/horus_client.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

class TaskDto {
  TaskDto.fromJson(Map<String, Object?> json)
    : id = json['id'] as int,
      title = json['title'] as String;

  final int id;
  final String title;
}

final showTask = Endpoint<TaskDto>(
  method: 'GET',
  path: '/tasks/{id}',
  decode: (json) => TaskDto.fromJson(json as Map<String, Object?>),
);

void main() {
  test(
    'resolves path and query parameters and decodes a typed response',
    () async {
      final transport = MockClient((request) async {
        expect(request.method, 'GET');
        expect(
          request.url.toString(),
          'https://api.example.com/v1/tasks/7?done=true',
        );
        expect(request.headers['authorization'], 'Bearer token');
        return http.Response(jsonEncode({'id': 7, 'title': 'Ship it'}), 200);
      });
      final client = HorusClient(
        Uri.parse('https://api.example.com/v1'),
        httpClient: transport,
        headers: {'authorization': 'Bearer token'},
      );

      final task = await client.send(
        showTask,
        pathParameters: {'id': 7},
        queryParameters: {'done': true, 'empty': null},
      );

      expect(task.id, 7);
      expect(task.title, 'Ship it');
    },
  );

  test(
    'JSON-encodes request bodies and surfaces non-success responses',
    () async {
      final transport = MockClient((request) async {
        expect(jsonDecode(request.body), {'title': 'New'});
        expect(request.headers['content-type'], contains('application/json'));
        return http.Response(jsonEncode({'message': 'invalid'}), 422);
      });
      final client = HorusClient(
        Uri.parse('https://api.example.com'),
        httpClient: transport,
      );
      final createTask = Endpoint<TaskDto>(
        method: 'POST',
        path: '/tasks',
        decode: (json) => TaskDto.fromJson(json as Map<String, Object?>),
      );

      await expectLater(
        client.send(createTask, body: {'title': 'New'}),
        throwsA(
          isA<HorusClientException>()
              .having((e) => e.statusCode, 'status', 422)
              .having((e) => e.body, 'body', {'message': 'invalid'}),
        ),
      );
    },
  );

  test('headersProvider is consulted for every request', () async {
    // A fixed header map freezes at construction, which cannot express a
    // token that expires between two calls.
    var token = 'first';
    final seen = <String>[];
    final client = HorusClient(
      Uri.parse('https://api.example.com'),
      httpClient: MockClient((request) async {
        seen.add(request.headers['authorization']!);
        return http.Response(jsonEncode({'id': 1, 'title': 'x'}), 200);
      }),
      headersProvider: () async => {'authorization': 'Bearer $token'},
    );

    await client.send(showTask, pathParameters: {'id': 1});
    token = 'refreshed';
    await client.send(showTask, pathParameters: {'id': 1});

    expect(seen, ['Bearer first', 'Bearer refreshed']);
  });

  test('a per-call header still wins over both sources', () async {
    final client = HorusClient(
      Uri.parse('https://api.example.com'),
      httpClient: MockClient((request) async {
        expect(request.headers['authorization'], 'Bearer explicit');
        return http.Response(jsonEncode({'id': 1, 'title': 'x'}), 200);
      }),
      headers: {'authorization': 'Bearer fixed'},
      headersProvider: () async => {'authorization': 'Bearer provided'},
    );

    await client.send(
      showTask,
      pathParameters: {'id': 1},
      headers: {'authorization': 'Bearer explicit'},
    );
  });

  test('X-Socket-ID is sent only while a socket id exists', () async {
    String? socketId;
    final seen = <String?>[];
    final client = HorusClient(
      Uri.parse('https://api.example.com'),
      httpClient: MockClient((request) async {
        seen.add(request.headers['x-socket-id']);
        return http.Response(jsonEncode({'id': 1, 'title': 'x'}), 200);
      }),
      socketIdProvider: () => socketId,
    );

    await client.send(showTask, pathParameters: {'id': 1});
    socketId = '1234.5678';
    await client.send(showTask, pathParameters: {'id': 1});

    // The header is what `PendingBroadcast.toOthers(request)` reads, so a
    // client with no socket open must not send a stale one.
    expect(seen, [null, '1234.5678']);
  });

  test('rejects a missing required path parameter before sending', () async {
    final client = HorusClient(
      Uri.parse('https://api.example.com'),
      httpClient: MockClient((_) async => fail('request must not be sent')),
    );

    await expectLater(
      client.send(showTask),
      throwsA(
        isA<ArgumentError>().having(
          (e) => e.message,
          'message',
          contains('id'),
        ),
      ),
    );
  });
}
