# SnF

C++20과 Linux `epoll`로 만든 실시간 TCP 게임 서버 연습 프로젝트다. Network Reactor 하나와
기본 2-Worker Sharded `ActorRuntime`을 분리했으며, PING/PONG, graceful shutdown,
non-blocking 부하 테스트 클라이언트와 1,000개 연결 승인 시험을 제공한다.

## 현재 기능

### 서버

- non-blocking listener와 `accept4()`
- level-triggered epoll event loop
- RAII 기반 client FD와 Session 수명 관리
- 길이 기반 binary Frame decode
- 부분 수신과 여러 Frame 일괄 수신
- Reactor → `CommandIngress` → ActorId shard별 bounded queue → ActorWorker → outbound bounded
  queue 구조
- `ConnectionId(fd, generation)`으로 늦은 응답과 FD 재사용 응답 차단
- 연결 generation을 임시 `ActorId`로 사용하며, 같은 Actor의 명령은 항상 같은 Worker FIFO에서
  처리
- Worker별 독립 `MessageDispatcher`가 `unordered_map<MessageType, Handler>` 기반 PING/PONG
  처리를 담당
- outbound `eventfd` wake-up과 Reactor의 Session 송신 queue 반영
- 해당 Actor shard queue 포화 시 그 연결만 종료하며, 종료 중 `Closed` 게시 결과는 overflow로
  세지 않음
- 기본값은 `actor_worker_count=2`, `actor_queue_capacity_per_worker=4096`이며 0은 허용하지 않음
- Worker별 accepted/processed/rejected-full, 현재 depth/high-water mark, 평균/최대 queue wait
  snapshot 제공
- 부분 송신 queue와 동적 `EPOLLOUT`
- Session별 기본 1 MiB 백프레셔
- client socket `TCP_NODELAY`
- `eventfd` 기반 `requestStop()`
- `signalfd` 기반 SIGINT·SIGTERM
- 기본 5초 ActorRuntime drain → `GameRuntimeDrained` 1회 → pending send drain graceful shutdown
- Worker 실패는 `ActorRuntimeFailed`를 발행해 grace period를 기다리지 않고 즉시 종료
- 종료 시 수락·종료 연결, 송수신 Frame, protocol error, queue overflow, stale action 및 Worker별
  ActorRuntime 통계 출력

### 부하 테스트 클라이언트

- 여러 non-blocking 연결을 하나의 epoll에서 처리
- `EINPROGRESS`와 `getsockopt(SO_ERROR)` 연결 완료 확인
- `timerfd` 기반 연결별 요청 속도 제어
- 연결별 증가 request ID와 monotonic timestamp payload
- connect/request timeout
- 응답 type, request ID, payload 검증
- 연결·요청·오류·처리량 통계
- 평균 및 p50/p95/p99 RTT

## Wire format

```text
[body_length:u32][type:u16][request_id:u32][payload]
```

- 모든 정수는 big-endian이다.
- `body_length`는 길이 필드 자신을 제외한다.
- body는 최소 6 byte, 최대 64 KiB다.
- `PING=1`, `PONG=2`다.

## Docker 개발 환경

이미지를 한 번 빌드한다.

```bash
docker build -t snf-server-dev .
```

프로젝트를 mount한 개발 container를 실행한다.

```bash
docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev
```

macOS에는 epoll이 없으므로 서버와 부하 클라이언트 실행은 Ubuntu 24.04 container 안에서
수행한다.

## 빌드와 테스트

Debug 단위·통합 테스트:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

ASan·UBSan 검증:

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan --output-on-failure
```

Release 실행 파일:

```bash
cmake --preset release
cmake --build --preset release
```

Release preset은 `assert` 기반 테스트를 만들지 않고 실행 파일만 최적화해 빌드한다.

## 실행

첫 번째 terminal에서 서버를 실행한다.

```bash
./build/release/snf_server
```

두 번째 terminal에서 부하 클라이언트를 실행한다.

```bash
./build/release/snf_load_client \
  --host 127.0.0.1 \
  --port 7777 \
  --connections 1000 \
  --duration 30 \
  --requests-per-second 10 \
  --connect-timeout-ms 5000 \
  --request-timeout-ms 3000
```

CLI 기본값:

| 옵션 | 기본값 |
| --- | ---: |
| `--host` | `127.0.0.1` |
| `--port` | `7777` |
| `--connections` | `100` |
| `--duration` | `30`초 |
| `--requests-per-second` | 연결당 `1` |
| `--connect-timeout-ms` | `5000` |
| `--request-timeout-ms` | `3000` |

현재 host는 DNS 이름이 아닌 numeric IPv4 주소를 받는다.

## 첫 마일스톤 승인 결과

2026-07-28, Ubuntu 24.04 Docker container의 Release 빌드에서 1,000개 연결과 연결당
초당 10개 요청(총 10,000 responses/s)으로 다음 결과를 확인했다.

```text
Connections: 1000/1000 succeeded, 0 failed, peak active 1000
Requests: 30000 sent, 30000 received, 0 timeout, 0 invalid, 0 socket error
Throughput: 10000.000 responses/s
RTT ms: avg 9.408, p50 7.496, p95 14.834, p99 65.433
Server summary: 1000 accepted, 1000 closed, 30000 frames received,
30000 frames sent, 0 protocol errors, 0 actor queue overflows,
0 stale network actions
```

Debug 및 ASan·UBSan 구성에서도 단위 테스트, TCP loopback 테스트, 느린 client 백프레셔,
pending send graceful shutdown, load client 통합 테스트가 모두 통과했다.

## Sharded ActorRuntime 승인 결과

2026-07-29, 기본 2-Worker 구성에서 Actor별 FIFO·단일 실행, shard 간 병렬 실행, shard별
포화 격리, close drain/cancel discard, Worker 예외 전파와 queue metric 단위 테스트를 추가했다.
기존 PING/PONG, stale response, inbound overflow, slow client, graceful shutdown 통합 테스트도
같은 구성으로 통과했다.

Ubuntu 24.04 Docker container에서 Debug 및 ASan·UBSan 전체 테스트(각 3개 target)와 Release
1,000 연결·30초 부하를 다시 검증했다.

```text
Connections: 1000/1000 succeeded, 0 failed, peak active 1000
Requests: 300000 sent, 300000 received, 0 timeout, 0 invalid, 0 socket error
Throughput: 10000.000 responses/s
RTT ms: avg 12.198, p50 12.105, p95 17.222, p99 19.778
Server summary: 1000 accepted, 1000 closed, 300000 frames received,
300000 frames sent, 0 protocol errors, 0 actor queue overflows,
0 stale network actions
```

Worker별로 응답 순서는 보장하지만 shard 사이의 전역 응답 순서는 정의하지 않는다.

## 주요 source

| 영역 | 위치 |
| --- | --- |
| Frame과 codec | `include/snf/protocol/`, `src/protocol/` |
| FD와 socket 설정 | `include/snf/net/`, `src/net/` |
| Session | `include/snf/net/session.hpp`, `src/net/session.cpp` |
| 메시지 dispatcher | `include/snf/server/message_dispatcher.hpp`, `src/server/message_dispatcher.cpp` |
| epoll 서버 | `include/snf/server/tcp_server.hpp`, `src/server/tcp_server.cpp` |
| 부하 클라이언트 | `include/snf/load/`, `src/load/`, `tools/load_client/` |
| 단위·통합 테스트 | `tests/` |
