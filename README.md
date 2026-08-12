# SnF

SnF는 C++20을 활용해 MORPG 콘텐츠의 상태, 규칙과 메시지 흐름을 설계하고 구현하는
프로젝트다. 서버 코어 자체를 계속 확장하는 것보다, Player와 Zone을 비롯한 게임 콘텐츠를
명확한 상태 소유권과 실행 경계 위에 올리는 것을 주된 목적으로 한다.

현재는 Linux `epoll` 네트워크 런타임과 coroutine suspend/resume을 지원하는 일반화된 sharded Actor
Runtime 위에서 PING/PONG vertical slice를 실행한다. outbound는 non-blocking 예약으로 동작한다. 다음으로
network correctness를 정리한 뒤 인증·영속성·Zone 이동을 하나의 playable vertical slice로 구현한다.
`ConnectionScope`와 UnifiedRuntime은 실제 콘텐츠 부하가 필요를 증명할 때 진행할 선택적 최적화다.

## 프로젝트 목적

- 현대적인 C++로 게임 상태와 콘텐츠 규칙을 모델링한다.
- Player, Zone과 공유 콘텐츠를 Actor 단위의 순차 실행으로 구성한다.
- 네트워크, 게임 로직과 느린 외부 I/O 사이의 경계를 명시한다.
- 인증·영속성·이동·AOI·공유 콘텐츠를 작은 vertical slice로 구현하고 검증한다.
- 기능뿐 아니라 순서 보장, 수명, backpressure와 graceful shutdown까지 함께 다룬다.

## 개발 방식

프로젝트의 주된 학습·설계 영역은 C++을 활용한 게임 콘텐츠다. 콘텐츠의 상태 소유권,
규칙, 메시지와 처리 흐름은 직접 설계하고 구현한다.

서버 코어는 먼저 전체 아키텍처, public contract, 불변 조건과 완료 기준을 정한다. 그 경계
안에서 반복적인 세부 구현, 테스트 보강, 리팩터링과 문서 정리는 LLM에 위임한다. LLM이 만든
변경은 코드 리뷰와 단위·통합 테스트, sanitizer 및 필요한 부하 테스트를 통과한 뒤 받아들인다.

| 영역 | 중점 |
| --- | --- |
| 게임 콘텐츠 | 상태 모델, 규칙, 메시지 흐름과 C++ 구현 |
| 서버 코어 | 전체 방향, 계층 경계, 실행·수명·포화 계약 설계 |
| LLM 활용 | 정해진 계약 안의 세부 구현, 테스트, 리팩터링과 문서화 |

## 아키텍처

```text
Client
  ↓
epoll Network Runtime
  ↓ FrameEnvelope
ProtocolGateway / CommandRouter
  ↓ typed route
Actor-Bound Logic Runtime
  ├── PlayerActor
  └── ZoneActor, Shared Content Actor (예정)
  ↓ typed effect
OutboundSink
  ↓
Network Runtime
```

핵심 원칙은 다음과 같다.

- 네트워크 계층과 게임 콘텐츠의 상태 소유권을 분리하고, Connection task는 게임 상태를 직접
  수정하지 않는다.
  - 현재는 Network Reactor와 Actor-Bound Logic Runtime이 별도 실행 영역을 사용한다.
  - 실제 부하가 필요를 증명하면 Connection, I/O continuation과 Actor turn의 실행 pool을
    통합할 수 있지만, typed command/effect 경계와 Actor별 상태 단일 소유권은 유지한다.
- Player, Zone과 공유 콘텐츠는 공통 Actor 실행 규칙을 사용한다. 현재 각 Actor의 mutable 상태는
  고정 Worker에서 FIFO로 처리하며, 향후 실행 pool을 바꾸더라도 Actor별 비동시 실행을 유지한다.
  - 이 프로젝트가 대상으로 하는 MORPG에서 이동 가능한 world 역할을 하는 lobby는 강한
    실시간 동기화가 필요하지 않다. 그 수준의 동기화가 필요한 game instance는 별도 서버로
    분리해 scale-out할 수 있으므로, 단일 프로세스에서는 여러 Actor 종류를 같은 Worker
    Pool에서 처리한다.
  - Actor turn budget으로 cooperative fairness를 제공하며, 외부 operation을 기다리는 Actor coroutine은
    suspend되어 같은 Worker의 다른 Actor가 진행한다. Actor 내부 mutex를 없애고 명령 순서와 cache
    locality를 보장하기 위한 선택이다.
  - 느린 handler가 같은 Worker의 다른 Actor를 지연시킬 수 있지만, DB 같은 외부 I/O는
    비동기로 실행해 Logic Worker가 대기하지 않게 한다.
- protocol Frame을 Actor까지 전달하지 않고 typed command와 effect 경계를 사용한다.
- queue와 in-flight operation에는 명시적인 상한과 포화 정책을 둔다.
- 외부 executor는 Actor 객체나 coroutine handle을 보유하지 않는다.
- 종료는 ingress close, Actor drain, pending send drain 순서를 명시적으로 따른다.

## 현재 상태

- non-blocking TCP listener와 level-triggered `epoll` reactor
- 길이 기반 binary Frame codec과 부분 수신·송신 처리
- 공통 `ProtocolGateway`와 typed command routing
- `ActorKey{ActorKind, EntityId}`로 sharding하는 2-Worker Actor-Bound Logic Runtime과 Actor별 FIFO mailbox
- Player 전용 `PlayerActorBinding`/`PlayerActorIngress`와 type-erased binding registry
- `PlayerActor` PING/PONG 처리와 typed result/effect 경계
- connection generation을 통한 stale response 차단
- bounded ingress queue와 Session별 send backpressure
- 용량 예약으로 동작하는 outbound channel: 포화 시 Worker가 아니라 Actor 하나가 suspend되고, 연결별
  상한과 reactor 회차당 grant 상한이 있으며, 예약 대기조차 승인되지 않으면 그 연결을 종료한다
- command마다 정확히 한 번 관측되는 credit 반환 신호와, 그것과 분리해 집계하는 admission 거부 지표
- connection lifecycle 전달, runtime drain/failure와 graceful shutdown
- lazy `ActorTask`, bounded continuation reservation과 owning-Worker 전용 coroutine resume/cancel/frame 파괴
- suspend 중 같은 Actor의 FIFO를 보존하면서 같은 Worker의 다른 Actor를 진행시키는 scheduler
- in-flight, suspension duration, reservation/cancel/late completion과 passivation 후보 metric
- reactor turn 지연, Actor queue wait, pending send, outbound depth와 outbound hand-off 시간의
  `p50/p95/p99/max` 계측과 운영 중 주기 노출
- 예약된 슬롯, 용량을 기다리는 Actor 수, 추적 중인 연결 수, 결과에 도달한 command 수와 admission
  거부 수 gauge
- 단위·TCP 통합·부하 테스트 및 ASan·UBSan·TSan preset

Phase 3.8에서 scheduler의 Player 전용 의존을 제거하고, 모든 Worker를
`ActorKeyHash(key) % worker_count`로 선택하는 Actor-Bound Logic Runtime으로 일반화했다.
현재 production binding은 Player 하나이며 ZoneActor와 timer는 이후 단계의 범위다.

Phase 3.9에서는 포화 정책의 현재 동작과 목표 동작을 계약으로 고정하고 baseline metric을 확보했다.
non-blocking outbound는 4.1에서 구현했다. in-flight credit은 실제로 느린 Player command가 등장하는
Playable Session slice에서 연결 간 격리 필요를 측정한 뒤 현재 reactor에 적용한다.

Phase 4.0에서는 `PlayerActor` handler를 lazy coroutine으로 전환하고 domain-agnostic async operation,
continuation, cancel과 drain 기계를 구현했다. PING에는 await할 작업이 없어 handler 자체는 동기 완료한다.

Phase 4.1에서는 그 기계의 첫 production 적용으로 blocking outbound를 제거했다. Binding이 command를
`Handling → Reserving` 두 단계로 실행해 handler가 결정을 끝낸 뒤에 용량을 얻으므로, `PlayerActor`와
`PlayerResult`는 outbound 용량이 유한하다는 사실을 알지 않는다. 포화가 아니면 예약이 즉시 성공해
operation을 시작하지 않고, 포화일 때만 그 Actor 하나가 suspend된다. Actor drain이 outbound 소비에
의존하게 되므로 종료 순서 제약도 함께 고정했다.

## 로드맵

```text
3.8 Actor-Bound Logic Runtime 일반화
→ 3.9 Backpressure 계약과 계측
→ 4.0 Actor Coroutine (Suspend / Resume)
→ 4.1 Async Outbound Reservation
→ 4.2 Network Correctness
→ 5 Playable Player Session
→ 6 Transactional Gameplay
→ 7 Shared Content와 Projection
→ 측정 후 Runtime 최적화 (선택)
```

상세 단계와 완료 기준은 [개발 로드맵](docs/development-roadmap.md), 목표 구조와 상태 소유권은
[서버 아키텍처 초안](docs/server-architecture-draft.md), coroutine 수명 규약은
[Coroutine Actor 계약](docs/coroutine-actor-contract.md), 전체 종료 판정과 실패·취소 전파는
[Runtime Lifecycle 계약](docs/runtime-lifecycle-contract.md)을 기준으로 한다.
[ConnectionScope 계약](docs/connection-scope-contract.md)과
[UnifiedRuntime 전환 개요](study/10-unified-runtime-overview.md)는 Playable Session과 Zone 부하가 실제
runtime 병목을 증명할 때 사용할 선택적 최적화 트랙의 설계 입력이다.

## 빌드와 테스트

서버는 Linux `epoll`을 사용한다. macOS에서는 Ubuntu 24.04 Docker container 안에서 빌드하고
실행한다.

```bash
docker build -t snf-server-dev .

docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev
```

Debug 빌드와 테스트:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

Sanitizer 검증:

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan --output-on-failure

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

Release 빌드:

```bash
cmake --preset release
cmake --build --preset release
```

## 실행

서버:

```bash
./build/release/snf_server
```

부하 테스트 클라이언트:

```bash
./build/release/snf_load_client \
  --host 127.0.0.1 \
  --port 7777 \
  --connections 1000 \
  --duration 30 \
  --requests-per-second 10
```

현재 wire format은 다음과 같다.

```text
[body_length:u32][type:u16][request_id:u32][payload]
```

모든 정수는 big-endian이며 body는 최대 64 KiB다. 현재 메시지는 `PING=1`, `PONG=2`를
사용한다.

## 디렉터리

| 위치 | 내용 |
| --- | --- |
| `include/snf/`, `src/` | core, network, protocol과 server runtime |
| `tests/` | 단위·통합 테스트 |
| `tools/load_client/` | non-blocking 부하 테스트 클라이언트 |
| `docs/` | 아키텍처, 로드맵, coroutine·runtime lifecycle·ConnectionScope 계약 |
