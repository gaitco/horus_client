import 'dart:math';

/// How long to wait before reconnect attempt [attempt] (1 is the first).
typedef Backoff = Duration Function(int attempt);

/// Exponential backoff with full jitter.
///
/// Jitter is not decoration: without it, every client dropped by the same
/// server restart comes back at the same instant and knocks it over again.
Backoff exponentialBackoff({
  Duration base = const Duration(milliseconds: 500),
  Duration cap = const Duration(seconds: 30),
  Random? random,
}) {
  final rng = random ?? Random();
  return (attempt) {
    // Shifting past 2^30 overflows on the web's 32-bit ints long before the
    // cap matters, so the exponent is clamped rather than the result alone.
    final exponent = min(attempt - 1, 30);
    final ceiling = min(base.inMilliseconds << exponent, cap.inMilliseconds);
    return Duration(milliseconds: rng.nextInt(ceiling + 1));
  };
}
