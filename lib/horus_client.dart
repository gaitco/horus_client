library;

import 'dart:convert';

import 'package:http/http.dart' as http;

typedef JsonDecoder<T> = T Function(Object? json);

/// Transport details for one generated API method.
final class Endpoint<T> {
  Endpoint({required String method, required this.path, required this.decode})
    : method = method.toUpperCase();

  final String method;
  final String path;
  final JsonDecoder<T> decode;

  String resolvePath(Map<String, Object?> parameters) {
    return path.replaceAllMapped(RegExp(r'\{(\w+)\}'), (match) {
      final name = match[1]!;
      final value = parameters[name];
      if (value == null) {
        throw ArgumentError('Missing required path parameter [$name].');
      }
      return Uri.encodeComponent(value.toString());
    });
  }
}

/// Headers computed per request — a bearer token that expires, for one.
typedef HeadersProvider = Future<Map<String, String>> Function();

/// The id of the live realtime connection, or null when there is none.
typedef SocketIdProvider = String? Function();

/// Small cross-platform HTTP runtime used by generated Maat clients.
final class HorusClient {
  HorusClient(
    this.baseUri, {
    http.Client? httpClient,
    Map<String, String> headers = const {},
    this.headersProvider,
    this.socketIdProvider,
  }) : _http = httpClient ?? http.Client(),
       _headers = Map.unmodifiable(headers);

  final Uri baseUri;
  final http.Client _http;
  final Map<String, String> _headers;

  /// Consulted before every request, so a token refreshed between two calls
  /// reaches the second one. A fixed [headers] map cannot express that.
  HeadersProvider? headersProvider;

  /// Sends `X-Socket-ID` while a realtime connection is open, which is the
  /// header `PendingBroadcast.toOthers(request)` reads to skip the client
  /// that caused the change. Set by [HorusRealtime]; leaving it null simply
  /// sends no header.
  SocketIdProvider? socketIdProvider;

  /// The headers this client would send for a request carrying [headers].
  ///
  /// Public because the realtime client authenticates its private channels
  /// through the same credentials, and duplicating that resolution is how the
  /// two drift apart.
  Future<Map<String, String>> resolveHeaders([
    Map<String, String> headers = const {},
  ]) async {
    return {
      'accept': 'application/json',
      ..._headers,
      ...?await headersProvider?.call(),
      'x-socket-id': ?socketIdProvider?.call(),
      ...headers,
    };
  }

  Future<T> send<T>(
    Endpoint<T> endpoint, {
    Map<String, Object?> pathParameters = const {},
    Map<String, Object?> queryParameters = const {},
    Object? body,
    Map<String, String> headers = const {},
  }) async {
    final path = endpoint.resolvePath(pathParameters);
    final base = baseUri.toString().replaceFirst(RegExp(r'/$'), '');
    final relative = path.replaceFirst(RegExp(r'^/'), '');
    final query = <String, dynamic>{
      for (final entry in queryParameters.entries)
        if (entry.value != null)
          entry.key: entry.value is Iterable
              ? (entry.value as Iterable)
                    .map((value) => value.toString())
                    .toList()
              : entry.value.toString(),
    };
    final uri = Uri.parse(
      '$base/$relative',
    ).replace(queryParameters: query.isEmpty ? null : query);
    final request = http.Request(endpoint.method, uri)
      ..headers.addAll(await resolveHeaders(headers));
    if (body != null) {
      request
        ..headers.putIfAbsent(
          'content-type',
          () => 'application/json; charset=utf-8',
        )
        ..body = jsonEncode(body);
    }

    final response = await http.Response.fromStream(await _http.send(request));
    final decoded = _decode(response.body);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw HorusClientException(
        response.statusCode,
        decoded,
        response.headers,
      );
    }
    return endpoint.decode(decoded);
  }

  void close() => _http.close();

  static Object? _decode(String body) {
    if (body.isEmpty) return null;
    try {
      return jsonDecode(body);
    } on FormatException {
      return body;
    }
  }
}

final class HorusClientException implements Exception {
  const HorusClientException(this.statusCode, this.body, this.headers);

  final int statusCode;
  final Object? body;
  final Map<String, String> headers;

  @override
  String toString() => 'HorusClientException($statusCode, $body)';
}
