import Foundation
import Testing
import PhoenixNectar

/// Uses only the public API (no `@testable`) so the custom transport surface cannot silently regress.
@Suite(.timeLimit(.minutes(1)))
struct PublicTransportTests {
  actor RecordingTransport: PhoenixTransport {
    let url: URL
    private var state: PhoenixTransportReadyState = .closed
    private var continuation: AsyncStream<TransportEvent>.Continuation?

    init(url: URL) {
      self.url = url
    }

    func readyState() async -> PhoenixTransportReadyState {
      state
    }

    func events(bufferingPolicy: AsyncStream<TransportEvent>.Continuation.BufferingPolicy) async -> AsyncStream<TransportEvent> {
      let (stream, continuation) = AsyncStream<TransportEvent>.makeStream(bufferingPolicy: bufferingPolicy)
      self.continuation = continuation
      return stream
    }

    func connect(with headers: [String: String]) async {
      state = .open
      continuation?.yield(.open)
    }

    func disconnect(code: Int, reason: String?) async {
      state = .closed
      continuation?.yield(.close(code, reason))
      continuation?.finish()
      continuation = nil
    }

    func sendText(data: Data) async {}
    func sendBinary(data: Data) async {}
  }

  final class URLRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var urls: [URL] = []

    func record(_ url: URL) {
      lock.lock()
      urls.append(url)
      lock.unlock()
    }

    var recorded: [URL] {
      lock.lock()
      defer { lock.unlock() }
      return urls
    }
  }

  @Test
  func socketConnectsThroughCustomTransport() async throws {
    let recorder = URLRecorder()
    let socket = try Socket(
      endpoint: "wss://example.com/socket",
      transportFactory: { url in
        recorder.record(url)
        return RecordingTransport(url: url)
      }
    )

    let states = await socket.connectionStateStream()
    try await socket.connect()

    var connected = false
    for await state in states {
      if case .connected = state {
        connected = true
        break
      }
    }

    #expect(connected)
    #expect(recorder.recorded.count == 1)
    #expect(recorder.recorded.first?.host == "example.com")

    await socket.disconnect()
  }

  @Test
  func typedConnectParamsOverloadPassesParamsToTransportFactory() async throws {
    struct Params: Encodable, Sendable { let token: String }

    let recorder = URLRecorder()
    let socket = try Socket(
      endpoint: "wss://example.com/socket",
      connectParamsProvider: { Params(token: "abc") },
      transportFactory: { url in
        recorder.record(url)
        return RecordingTransport(url: url)
      }
    )

    try await socket.connect()

    #expect(recorder.recorded.first?.query?.contains("token=abc") == true)

    await socket.disconnect()
  }

  @Test
  func urlSessionTransportIsConstructibleAsTransport() {
    let transport: any PhoenixTransport = URLSessionTransport(
      url: URL(string: "wss://example.com/socket")!,
      configuration: .ephemeral
    )
    _ = transport
  }
}
