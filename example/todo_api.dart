import 'package:maat_horus/maat_horus.dart';

final class TaskDto {
  const TaskDto({required this.id, required this.title});

  factory TaskDto.fromJson(Map<String, Object?> json) =>
      TaskDto(id: json['id'] as int, title: json['title'] as String);

  final int id;
  final String title;
}

final _showTask = Endpoint<TaskDto>(
  method: 'GET',
  path: '/tasks/{id}',
  decode: (json) => TaskDto.fromJson(json as Map<String, Object?>),
);

/// This is the small wrapper shape Maat's OpenAPI generator should emit.
final class TodoApi {
  TodoApi(this._client);

  final HorusClient _client;

  Future<TaskDto> showTask(int id) =>
      _client.send(_showTask, pathParameters: {'id': id});
}

void main() {
  final transport = HorusClient(Uri.parse('https://api.example.com'));
  final api = TodoApi(transport);
  print(api.showTask);
  transport.close();
}
