# SnF

SnF는 C++20을 활용해 MORPG 콘텐츠의 상태, 규칙과 메시지 흐름을 설계하고 구현하는
프로젝트다. 서버 코어 자체를 계속 확장하는 것보다, Player와 Zone을 비롯한 게임 콘텐츠를
명확한 상태 소유권과 실행 경계 위에 올리는 것을 주된 목적으로 한다.

현재는 Linux `epoll` 네트워크 런타임과 coroutine suspend/resume을 지원하는 일반화된 sharded Actor
Runtime 위에서 인증, Player 영속성, Zone 이동·tick과 멱등한 구매 vertical slice를
실행한다. outbound와 repository 대기는 Actor 하나만 suspend하는 non-blocking 경계를
사용한다. MySQL durable adapter와 Shared Content·Projection, durable ranking outbox/checkpoint까지
완료했으며, cross-zone handoff의 정상 경로, target 실패 보상과 transition 중
disconnect·shutdown 정리까지 연결했다. 현재 콘텐츠 로드맵의 필수 단계는 완료됐고,
후속 Runtime 변경은 측정이 필요를 증명할 때 진행하는 선택적 최적화다.
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
  ├── ZoneActor
  └── PartyActor
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
- Player·Zone·Party typed binding/ingress와 type-erased binding registry
- 주입 가능한 clock과 stale `TimerId` 폐기를 갖춘 bounded Zone timer scheduler
- disconnect/save/reconnect 뒤 복원되는 Player의 마지막 Zone 위치
- 재화 차감·상품 지급·idempotency 증거를 원자적으로 적용하는 bounded 구매
  repository와 wire command
- 전용 bounded Worker Pool에서 blocking MySQL C API를 실행하는 durable Player repository:
  Player snapshot과 구매 idempotency를 InnoDB에 저장하고 unique key 경합과 crash/retry를 복구
- Zone command/tick 실행 `p50/p95/p99/max`와 tick budget overrun metric
- `PlayerActor` PING/PONG 처리와 typed result/effect 경계
- 인증된 Player의 typed 구매 command, repository await와 응답 유실·reconnect retry에서
  effectively-once effect를 검증하는 purchase result
- 두 Player가 공유하는 bounded PartyActor membership, typed `PartyFull`, disconnect leave와
  mailbox-safe empty Party passivation
- Player score/sequence와 event를 원자적으로 기록하는 durable ranking outbox, startup/live tail을
  strict offset 순서로 적용하는 전용 projector와 schema v4 generation checkpoint 복구
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
현재 production path는 Player와 Zone binding, 인증 session route, Zone enter/move/leave wire 왕복과
Zone별 periodic timer, 위치 영속 복원과 mailbox-safe 빈 Zone passivation을 포함한다. hot Zone 입력
coalescing은 이후 단계의 범위다.

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

Phase 6에서는 구매의 재화 차감, 상품 지급과 idempotency 증거를 하나의
repository transaction으로 묶고, repository 대기 중에도 같은 Worker의 다른 Actor가
진행하는 wire vertical slice를 추가했다. 기본값은 결정적 in-memory adapter이고,
`SNF_MYSQL_HOST`를 설정하면 bounded MySQL adapter를 사용한다. MySQL adapter는 Player snapshot,
재화 차감, 상품 지급과 idempotency row를 InnoDB transaction으로 저장해 프로세스 crash 전후의
retry를 복구한다.

Phase 7.1에서는 공유 콘텐츠의 첫 vertical slice로 PartyActor를 추가했다. PartyId별
FIFO mailbox가 membership을 변경하고, coordinator의 session route와 membership epoch이
stale leave를 차단한다. 용량 초과는 연결 종료가 아닌 typed `PartyFull`로 응답하며,
마지막 leave 후 빈 Actor를 회수한다.

Phase 7.2에서는 절대 score와 Player별 단조 sequence, strict global offset을 적용하는 결정적 ranking
projection과 checkpoint/tail replay 의미를 in-memory reference로 검증했다. Phase 7.3a는 trusted
award에 durable identity를 부여하고 Player score/sequence와 outbox event를 schema v2 MySQL
transaction 하나로 commit한다.

Phase 7.3 계약은 strict global offset projector와 checkpoint/shutdown 복구 순서까지 고정했다.
7.3b는 durable checkpoint와 전용 projector를 추가했다. 서버는 gameplay ingress 전에 checkpoint와
durable tail을 동기 복구하고, 실행 중 bounded polling으로 새 event를 적용하며, Actor/repository drain
뒤 마지막 tail과 checkpoint를 확정한다. 7.3c 부하 측정으로 strict cursor의 약 1.1k awards/s 포화와
full-snapshot checkpoint의 Player 수 비례 비용을 확인했다. schema v4 generation swap은 snapshot 작성
중 stream lock을 제거했고, retention은 season identity/archive 계약 전까지 outbox 전체 보존으로 정했다.

콘텐츠 부하를 재현하기 위해 load client는 기존 `ping` 외에 `zone` 시나리오를 제공한다. 각 연결이
고유 Player로 인증하고 Zone에 입장한 뒤 지속 이동하며 bootstrap과 gameplay RTT를 분리한다. 현재
Release 기준선(200 connections, 8 Zones, 12초, 연결당 20 req/s)은 48,000/48,000 응답,
gameplay p99 3.705ms, timeout·queue overflow·tick overrun 0이었다. reactor turn p99 0.655ms와
균형 잡힌 Worker 처리량을 함께 보면 지금은 ConnectionScope를 구현할 근거가 없으며, durable DB와
hot Zone 측정이 다음 판단 입력이다. 여기서 durable DB는 adapter 구현 여부가 아니라 MySQL을 붙인
playable 부하의 queue/operation 지연을 뜻한다.

같은 200-connection 조건에서 MySQL을 붙인 후속 측정은 47,800/47,800 응답, gameplay p99
10.654ms였고, 바로 이어 실행한 in-memory 대조군은 48,000/48,000, 10.209ms였다. MySQL operation
p99는 4.719ms, queue high-water는 로그인 burst에서 198이었지만 거부·실패는 없었다. 단일 Zone에
200명을 모은 측정도 48,000/48,000, gameplay p99 8.960ms, tick overrun 0이었다. 이때 Logic 처리량은
한 Worker에 집중되고 Zone command p99가 7.167µs로 올라 병목 방향은 Zone shard임을 보였지만,
현재 규모에서 분할·coalescing 또는 ConnectionScope를 정당화할 장애는 재현되지 않았다.

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
[Durable Ranking 계약](docs/durable-ranking-contract.md)은 Phase 7.3의 transaction, projector와
checkpoint 복구 기준이고, [Ranking 성능 기준선](docs/ranking-performance-baseline.md)은 cursor/checkpoint
측정과 retention 결정을 기록한다.
[Cross-Zone Handoff 계약](docs/cross-zone-handoff-contract.md)은 Phase 7.4의 transition state,
completion 상한, 보상과 disconnect/shutdown 순서를 정의한다.
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

서버를 실행한 상태에서 playable Zone 부하는 다음처럼 재현한다.

```bash
./build/release/snf_load_client \
  --scenario zone \
  --connections 200 \
  --players-per-zone 25 \
  --duration 12 \
  --requests-per-second 20
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

기본 서버는 in-memory repository를 사용한다. MySQL 8/InnoDB repository를 선택하려면 먼저
database와 사용자를 만든 뒤 환경 변수를 전달한다. schema version과 필요한 table은 서버 시작 시
확인·생성한다.

```bash
SNF_MYSQL_HOST=127.0.0.1 \
SNF_MYSQL_PORT=3306 \
SNF_MYSQL_USER=snf \
SNF_MYSQL_PASSWORD=secret \
SNF_MYSQL_DATABASE=snf \
./build/release/snf_server
```

MySQL 통합 테스트는 `SNF_MYSQL_TEST_HOST`가 없으면 skip된다. 실제 DB에 대해 실행할 때는 별도의
test database를 사용해야 한다. 테스트가 Player, purchase, ranking outbox/checkpoint table을 초기화하고
schema migration을 재현하므로 운영 database를 지정하면 안 된다.

```bash
SNF_MYSQL_TEST_HOST=127.0.0.1 \
SNF_MYSQL_TEST_USER=snf \
SNF_MYSQL_TEST_PASSWORD=secret \
SNF_MYSQL_TEST_DATABASE=snf_test \
./build/debug/snf_mysql_integration_tests
```

trusted ranking award와 projector/checkpoint 부하는 별도 database에서 다음처럼 재현한다.

```bash
SNF_MYSQL_HOST=127.0.0.1 \
SNF_MYSQL_USER=snf \
SNF_MYSQL_PASSWORD=secret \
SNF_MYSQL_DATABASE=snf_ranking_bench \
./build/release/snf_ranking_benchmark \
  --players 1000 --awards 5000 --workers 4 \
  --checkpoint-every 1000
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

모든 정수는 big-endian이며 body는 최대 64 KiB다. Player 메시지는
`PING=1`, `PONG=2`, `AUTHENTICATE=3`, `AUTHENTICATED=4`, `PURCHASE=11`,
`PURCHASE_RESULT=12`를 사용한다. Zone 메시지는 5~10을 사용한다.
Party 메시지는 `PARTY_JOIN=13`, `PARTY_JOINED=14`, `PARTY_LEAVE=15`,
`PARTY_LEFT=16`을 사용한다.

## 디렉터리

| 위치 | 내용 |
| --- | --- |
| `include/snf/`, `src/` | core, network, protocol과 server runtime |
| `tests/` | 단위·통합 테스트 |
| `tools/load_client/` | non-blocking 부하 테스트 클라이언트 |
| `tools/ranking_benchmark/` | durable award/projector/checkpoint 부하 측정 도구 |
| `docs/` | 아키텍처, 로드맵, coroutine·runtime lifecycle·ConnectionScope 계약 |
