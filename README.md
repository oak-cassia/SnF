# SnF

C++20과 Linux `epoll`로 만든 실시간 TCP 게임 서버 연습 프로젝트다. Network Reactor 하나와
기본 2-Worker sharded `ActorRuntime`을 분리했고, 공통 `ProtocolGateway`가 Frame을 typed command로
변환·라우팅한 뒤 Actor mailbox를 거쳐 `PlayerActor`가 PING/PONG을 처리한다. graceful shutdown,
non-blocking 부하 테스트 클라이언트와 1,000개 연결 승인 시험을 제공한다.

## 현재 기능

### 서버

- non-blocking listener와 `accept4()`
- level-triggered epoll event loop
- RAII 기반 client FD와 Session 수명 관리
- 길이 기반 binary Frame decode
- 부분 수신과 여러 Frame 일괄 수신
- Reactor → Frame decode → `FrameIngress` → `ProtocolGateway` → `RoutedCommand` →
  `CommandRouter` → `ProvisionalActorId` shard ingress → Actor별 FIFO mailbox → ready queue →
  `PlayerActor` → typed `PlayerResult` → `PlayerEffectSink` → response mapper → `OutboundSink` →
  bounded queue 구조
- `ConnectionId(fd, generation)`으로 늦은 응답과 FD 재사용 응답 차단
- 연결 generation을 인증 전 `ProvisionalActorId`로만 사용한다. 영속 `PlayerId`, DB key 또는
  재접속 복원 key로 사용하지 않으며, 같은 임시 Actor의 명령은 같은 Worker에서 FIFO로 처리한다.
- `TcpServer`는 protocol·Player 타입을 모르고 `FrameEnvelope`만 공통 Gateway에 게시한다.
- `ProtocolGateway`의 `MessageDispatcher`가 Actor 진입 전에 지원 여부와 payload를 검사하고,
  `CommandRouter`가 target과 command가 결합된 route를 Player ActorRuntime으로 전달한다.
  미지원·잘못된 메시지는 Actor를 생성하지 않는다.
- `PlayerActor`는 외부에서 수정할 수 없는 `PlayerState`를 소유하고, protocol Frame이 아닌 typed
  command를 멤버 handler에서 처리한다. typed `PlayerResult`는 wire response가 아닌 effect를 반환하고,
  별도 protocol effect sink가 handler 완료 후 Frame으로 변환한다. 현재 상태는 처리 command 수만 기록한다.
- Worker는 ingress를 한 번에 최대 64개 mailbox로 분배하고, ready Actor는 turn당 최대 16개를
  실행한다. backlog가 남은 Actor는 ready queue 끝에 한 번만 다시 등록한다.
- `Idle`, `Ready`, `Running`, `Suspended` 실행 상태를 정의한다. suspend/resume 진입은 coroutine
  단계에서 추가한다.
- `EventFdOutboundSink`가 outbound queue와 `eventfd` wake-up을 감추고, ActorRuntime은 네트워크
  backend의 queue나 descriptor를 직접 알지 않는다. queue 포화로 대기 중인 publish는 해당
  Runtime의 stop token으로 중단되며 공유 outbound queue는 다른 Runtime을 위해 유지된다.
- 해당 Actor shard queue 포화 시 그 연결만 종료하며, 종료 중 `Closed` 게시 결과는 overflow로
  세지 않음
- 연결 종료는 `ConnectionClosed{connection, cause}`로 기존 ingress FIFO의 해당 command 뒤에 전달한다.
  close를 소비한 owning worker만 actor를 evict하며, 포화된 lifecycle post는 reactor가 회차당 최대
  64건씩 재시도한다. active session과 pending close는 기본 4,096개의 lifecycle slot을 공유하며,
  소진 시 새 연결을 Actor 생성 전에 거부한다. close는 command
  accepted/processed/rejected-full·queue-wait 통계를 오염시키지 않는다.
- 기본값은 `actor_worker_count=2`, `actor_queue_capacity_per_worker=4096`이며 0은 허용하지 않음.
  이 capacity는 ingress·mailbox·실행 중 command를 합친 Worker별 outstanding 상한이다.
- Worker별 accepted/processed/rejected-full/evicted actor, outstanding depth/high-water mark, actor/ready actor
  수, mailbox depth/high-water mark, budget yield turn, 평균/최대 queue wait snapshot 제공
- 부분 송신 queue와 동적 `EPOLLOUT`
- Session별 기본 1 MiB 백프레셔
- client socket `TCP_NODELAY`
- `eventfd` 기반 `requestStop()`
- `signalfd` 기반 SIGINT·SIGTERM
- runtime별 drain/failure는 outbound action과 분리된 `RuntimeCompletionCoordinator`가 atomic
  상태로 추적한다. eventfd는 wake-up hint일 뿐이며 outbound 포화로 완료 상태가 유실되지 않는다.
- 기본 5초 ActorRuntime drain → 모든 필수 Runtime drain 확인 → pending send drain graceful shutdown
- Worker 실패는 completion coordinator를 통해 grace period를 기다리지 않고 즉시 종료
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

ThreadSanitizer 검증:

```bash
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

TSan은 ASan·UBSan과 별도 preset으로 실행하며 동시에 활성화할 수 없다.

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
0 stale outbound actions
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
0 stale outbound actions
```

Worker별로 응답 순서는 보장하지만 shard 사이의 전역 응답 순서는 정의하지 않는다.

## PlayerActor와 Mailbox 승인 결과

2026-07-29, Ubuntu 24.04 Docker container에서 공통 FrameIngress·ProtocolGateway·CommandRouter,
Actor 진입 전 typed command 변환, PlayerActor 멤버 handler와 typed PlayerResult, PING/PONG,
같은 Actor의 FIFO·정확히 한 번·단일 실행, mailbox cancel discard, 16-command turn yield,
worker별 outstanding 상한과 close drain을 검증했다. 미지원 메시지가 Actor를 생성하지 않는지
확인했고, 기존 Worker 예외 전파, stale response, slow client, graceful shutdown 회귀도 유지했다.

Debug 및 ASan·UBSan 구성의 전체 테스트는 각각 3개 target 모두 통과했다. Release 1,000 연결,
연결당 초당 10개 요청, 30초 부하 결과는 다음과 같다.

```text
Connections: 1000/1000 succeeded, 0 failed, peak active 1000
Requests: 299553 sent, 299553 received, 0 timeout, 0 invalid, 0 socket error
Throughput: 9985.100 responses/s
RTT ms: avg 9.157, p50 8.650, p95 12.579, p99 15.682
Server summary: 1000 accepted, 1000 closed, 299553 frames received,
299553 frames sent, 0 protocol errors, 0 actor queue overflows,
0 stale outbound actions
```

## Coroutine 계약 준비 승인 결과

2026-07-29, `ConnectionId`를 net 계층으로 이동하고 generation 기반 키를
`ProvisionalActorId`로 명확히 했다. `OutboundSink`가 queue와 wake-up을 캡슐화하며,
`RuntimeCompletionCoordinator`가 outbound 포화와 독립적으로 runtime별 drain/failure를
추적한다. Phase 4의 terminal continuation, frame 수명, cancel/late completion과 drain 규약은
`docs/coroutine-actor-contract.md`에 기록했다.

Ubuntu 24.04 Docker container에서 Debug, ASan·UBSan, TSan 구성의 전체 테스트가 각각 3개 target
모두 통과했다.

## 경계 강화 승인 결과

2026-07-29, `PlayerResult` effect 채널과 protocol adapter를 분리해 ActorRuntime의 `Frame` 및
outbound action 전이 의존을 제거했다. inbound command, outbound action, lifecycle, post result의
public 타입 헤더도 분리했고 `RuntimeId`는 runtime 구성 시 명시하도록 강제했다.

정상 peer disconnect, protocol error, actor ingress overflow는 원인과 `ConnectionId`를 보존한
`ConnectionClosed`로 gateway와 router를 거쳐 actor mailbox에 전달된다. actor가 없는 close는 slot을
만들지 않고, 기존 command 뒤의 close를 consuming worker가 actor를 evict한다. 포화 시 reactor retry는
FIFO를 다시 쓰지 않고 pending close만 회차당 64건까지 회전시킨다. active session과 pending close는
기본 4,096개의 bounded lifecycle slot을 공유하며, 소진 시 새 연결은 Actor 생성 전에 거부한다.

Ubuntu 24.04 Docker container에서 Debug, ASan·UBSan, TSan 전체 테스트(각 3개 target)를 통과했다.

## 주요 source

| 영역 | 위치 |
| --- | --- |
| Frame과 codec | `include/snf/protocol/`, `src/protocol/` |
| FD와 socket 설정 | `include/snf/net/`, `src/net/` |
| Session | `include/snf/net/session.hpp`, `src/net/session.cpp` |
| protocol gateway와 router | `include/snf/server/protocol_gateway.hpp`, `include/snf/server/command_router.hpp` |
| PlayerActor와 typed 결과 | `include/snf/server/player_actor.hpp`, `include/snf/server/player_result.hpp` |
| outbound와 runtime 완료 경계 | `include/snf/server/outbound_sink.hpp`, `include/snf/server/runtime_completion.hpp` |
| epoll 서버 | `include/snf/server/tcp_server.hpp`, `src/server/tcp_server.cpp` |
| 부하 클라이언트 | `include/snf/load/`, `src/load/`, `tools/load_client/` |
| 단위·통합 테스트 | `tests/` |
