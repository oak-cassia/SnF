# SnF 서버 아키텍처 초안

> 상태: Draft  
> 대상: C++20, Linux, `epoll` 기반 실시간 게임 서버  
> 목표: 월드와 인스턴스 전투가 공존하는 게임 서버의 실행 모델과 상태 소유권 정의
> 구현 순서는 [개발 로드맵](./development-roadmap.md)을 단일 기준으로 사용한다.

## 1. 문서 목적

이 문서는 SnF를 단순 패킷 서버에서 현대적인 실시간 게임 서버 구조로 발전시키기 위한
목표 아키텍처를 정의한다.

SnF의 최종 학습 목표는 다음과 같다.

- 네트워크 I/O와 게임 로직을 분리한다.
- 게임 상태마다 단 하나의 논리적 소유자를 둔다.
- 이벤트 기반 콘텐츠와 고정 tick 시뮬레이션을 서로 다른 실행 모델로 처리한다.
- 여러 Actor와 시뮬레이션 객체를 소수의 OS Worker Thread에서 실행한다.
- 느린 DB, 파일 로그, 외부 I/O가 게임 Worker를 막지 않게 한다.
- 모든 경계에 순서 보장, 수명 검증, backpressure와 관측 가능성을 둔다.
- 처음에는 단일 프로세스로 구현하되 나중에 프로세스를 분리할 수 있게 한다.

이 문서에서 말하는 최종 형태는 대규모 분산 시스템을 처음부터 구축한다는 뜻이 아니다.
우선 하나의 프로세스 안에서 모듈과 메시지 경계를 검증한 뒤, 필요할 때 같은 경계를
프로세스 간 통신으로 교체하는 것을 의미한다.

## 2. 핵심 결정

### 2.1 플랫폼 모델

SnF는 Linux 서버이므로 네트워크 계층은 `epoll` 기반 Reactor Group으로 시작한다. protocol gateway는
`FrameIngress`를, ActorRuntime은 `PlayerEffectSink`를 통해 network runtime과 만난다. 어느 쪽도
backend의 queue나 wake-up primitive를 직접 참조하지 않는다. io_uring은 콘텐츠 단계의 선행 조건이
아닌 선택적 두 번째 backend다.

```text
NetworkRuntime
├── Linux: EpollNetworkRuntime
└── Linux: IoUringNetworkRuntime (선택적 확장)
```

### 2.2 상태 소유권

게임 상태를 보호하는 기본 수단은 큰 mutex가 아니라 단일 소유권과 순차 실행이다.

- PlayerActor만 유저 영구 상태를 수정한다.
- Zone만 해당 필드의 실시간 상태를 수정한다.
- CombatRoom만 해당 전투의 실시간 상태를 수정한다.
- PartyActor, GuildActor 같은 공유 콘텐츠 Actor가 자신의 상태를 수정한다.
- 다른 Runtime의 mutable 객체를 직접 참조하지 않고 typed message를 보낸다.

### 2.3 Actor와 Thread

Actor는 OS Thread가 아니다. Actor는 상태, mailbox 또는 명령 처리 규칙을 가진 논리적
실행 단위다. 소수의 Worker Thread가 많은 Actor를 shard 방식으로 나누어 실행한다.

Zone과 CombatRoom은 유저 하나당 Actor를 만드는 방식보다 여러 Entity를 함께 소유하는
시뮬레이션 aggregate로 취급한다. 내부 hot path는 연속 저장 구조나 단순한 ECS 형태를
사용할 수 있다.

### 2.4 실행 모델

하나의 범용 Thread Pool에 모든 작업을 넣지 않는다. 작업 특성에 맞는 executor를 둔다.

| Executor | 용도 | 대표 객체 |
| --- | --- | --- |
| Reactor | 네트워크 readiness 처리 | Session |
| Event Actor Executor | 이벤트 기반 순차 상태 변경 | Player, Party, Guild |
| Fixed-Tick Executor | 주기적인 실시간 시뮬레이션 | Zone, CombatRoom |
| Blocking I/O Pool | 느린 외부 I/O | DB query, file operation |
| Stateless Job Pool | 선택적인 CPU 작업 | 경로 탐색, 압축 |

Stateless Job Pool은 게임 상태를 직접 수정하지 않는다. immutable snapshot을 입력으로 받고
결과를 원래 상태 소유자에게 메시지로 반환한다.

## 3. 전체 구조

```mermaid
flowchart LR
    Client["Game Client"] --> Network["Network Runtime<br/>epoll Reactor Group"]
    Network --> Router["Command Router<br/>세션 경로와 패킷 라우팅"]

    Router --> Player["Player Runtime<br/>Event Actor Shards"]
    Router --> World["World Runtime<br/>Zone Fixed-Tick Workers"]
    Router --> Battle["Battle Runtime<br/>Room Fixed-Tick Workers"]
    Router --> Content["Shared Content Runtime<br/>Party · Guild · Matchmaking"]

    World -->|BeginBattle| Player
    Player -->|CombatSnapshot| Battle
    Battle -->|BattleResult| Player
    Player -->|ReturnToWorld| World

    Player --> Async["Async Services"]
    Content --> Async
    Async --> DB["DB Worker Pool"]
    Async --> Timer["Timer Scheduler"]

    Player --> Network
    World --> Network
    Battle --> Network
    Content --> Network

    Network -.-> Observability["Log · Metrics · Tracing"]
    Player -.-> Observability
    World -.-> Observability
    Battle -.-> Observability
```

## 4. 런타임 구성

### 4.1 Main 및 Control

Main Thread는 다음만 담당한다.

- 설정 로드
- Runtime 생성과 시작 순서 관리
- signal 수신과 종료 요청
- graceful shutdown 조정
- 최종 통계 출력

일반 패킷이나 게임 콘텐츠를 Main Thread에서 처리하지 않는다.

### 4.2 Network Runtime

각 Reactor Worker는 자신의 `epoll` 인스턴스와 Session 집합을 독점한다.

```text
NetworkRuntime
├── ReactorWorker 0
│   ├── epoll fd
│   ├── Session A
│   └── Session B
└── ReactorWorker 1
    ├── epoll fd
    ├── Session C
    └── Session D
```

Network Runtime의 책임은 다음과 같다.

- 연결 수락과 종료
- non-blocking recv/send
- Frame 조립 및 wire format 검증
- `FrameEnvelope`를 공통 `ProtocolGateway`에 제출
- outbound queue와 slow client backpressure
- Gateway 결과에 따른 프로토콜 오류·포화 처리

Network Runtime은 인벤토리, 이동, 충돌, 전투 같은 게임 상태를 수정하지 않는다.

Actor handler는 `PlayerResult`에 domain effect를 반환하고, `PlayerEffectSink`가 handler 완료 뒤 이를
적용한다. 현재 `ProtocolPlayerEffectSink`가 `SendResponse`를 `ProtocolResponseMapper`로 `SendFrame`
으로 바꿔 `OutboundSink`에 게시한다. `EventFdOutboundSink`가 bounded queue와 `eventfd` wake-up을
캡슐화하며, 실제 Session 조회, encode와 send는 Reactor가 수행한다. runtime drained/failed는 outbound action과 분리된
`RuntimeCompletionCoordinator`가 추적한다. outbound queue 포화로 Worker가 대기할 때는 publishing
Runtime의 stop token이 그 대기만 중단하며, 다른 Runtime이 공유하는 sink와 queue는 유지한다.

FD는 재사용될 수 있으므로 다른 Runtime에는 raw FD 대신 다음과 같은 논리 ID를 전달한다.

```cpp
struct ConnectionId
{
    int descriptor;
    std::uint64_t generation;
};
```

응답 시 generation이 현재 Session과 다르면 오래된 응답으로 판단하여 폐기한다.

### 4.3 Command Router

현재 `ProtocolGateway`는 연결 generation에서 만든 `ProvisionalActorId`와 typed command 또는
`ConnectionClosed` lifecycle 사실을 route로 결합하고, `CommandRouter`가 Player ActorRuntime에
전달한다. 임시 ID는
인증 전 routing key일 뿐이며 `PlayerId`, DB key, 저장 key 또는 재접속 key가 아니다.

인증과 World/Battle 도입 후에는 `RouteCoordinator`가 `SessionRoute`와 `route_epoch`의
authoritative owner가 된다. Gateway는 route snapshot과 epoch을 command에 부여하고 destination은
자신의 activation epoch과 비교한다. epoch은 stale destination 검출 수단이며 route 전환의
원자성은 별도 transition protocol이 보장한다.

```cpp
struct SessionRoute
{
    UserId user_id;
    RouteKind kind;   // Player, World, Battle, SharedContent
    EntityId target;  // playerId, zoneId, roomId, partyId 등
    std::uint64_t epoch;
};
```

예시는 다음과 같다.

| 명령 | 목적지 |
| --- | --- |
| 장비 변경, 보상 수령 | PlayerActor |
| 필드 이동, 필드 상호작용 | Zone |
| 전투 이동, 공격 | CombatRoom |
| 파티 초대 | PartyActor |
| 길드 가입 | GuildActor |
| 매칭 참가 | MatchmakingActor |

라우팅 메타데이터는 게임 상태 전체의 복사본이 아니다. 실제 상태의 최종 권한은 목적지
Actor 또는 시뮬레이션 aggregate에 있다.

### 4.4 Player Runtime

PlayerActor는 다음 상태를 소유한다.

- 인벤토리, 장비, 재화
- 퀘스트, 업적, 영구 진행도
- 로그인 및 연결 상태
- `InWorld(zoneId)`, `InBattle(roomId)` 같은 상위 위치 상태
- dirty flag, entity version, 저장 진행 상태

Player Runtime은 `player_id`를 기준으로 shard한다.

```cpp
worker_index = hash(player_id) % player_worker_count;
```

동일 PlayerActor의 명령은 항상 같은 Worker에서 순차 실행한다. 인증 전 현재 구현은 연결
generation을 `ProvisionalActorId`로 사용해 기본 2개 Worker에 shard하고, Worker별 Actor
mailbox에서 typed command를 순차 실행한다. 영속 `PlayerId`는 인증 vertical slice에서 도입한다.

```cpp
player_runtime.post(player_id, PlayerCommand{...});
```

현재 Actor mailbox와 ready queue는 중복 ready token을 방지하고 turn당 16개 command budget을
적용한다. Actor 간 메시징과 복잡한 lifecycle이 필요해지면 `ActorRef::tell()`과 passivation을
추가한다.

`PlayerActor::state()`의 const 참조는 owning Worker 내부 테스트와 진단에만 사용한다. `const`는
thread-safe snapshot을 의미하지 않으므로 다른 thread의 상태 조회는 query command 또는 immutable
snapshot으로 수행한다.

### 4.5 World Runtime

World Runtime은 Zone 또는 MapInstance 단위로 실시간 상태를 소유한다.

- 필드 위치와 이동
- 필드 충돌
- AOI 및 주변 객체 갱신
- NPC와 AI
- 스폰과 despawn
- 전투 진입 판정

World Worker는 `zone_id`를 기준으로 Zone을 소유한다.

```cpp
worker_index = hash(zone_id) % world_worker_count;
```

한 tick의 권장 처리 순서는 다음과 같다.

```text
입력 command drain
→ 이동 입력 반영
→ 공간 인덱스 갱신
→ 충돌 처리
→ NPC와 AI
→ 스폰 처리
→ AOI 계산
→ 도메인 이벤트와 outbound 생성
```

이벤트만 필요한 비활성 콘텐츠는 전체 Zone tick에 넣지 않고 timer나 domain event로
처리할 수 있다.

### 4.6 Battle Runtime

Battle Runtime은 `room_id`를 기준으로 여러 CombatRoom을 Worker에 분산한다.

```cpp
worker_index = hash(room_id) % battle_worker_count;
```

CombatRoom은 다음을 소유한다.

- 전투 중 위치와 이동
- 전투 중 HP와 상태 이상
- 충돌과 공격 판정
- 전투 타이머
- 승패 및 종료 조건

전투 결과를 클라이언트에 자주 보내지 않더라도 서버 내부에서 연속적인 충돌과 이동을
계산한다면 fixed tick을 유지한다. 네트워크 송신 주기와 시뮬레이션 tick 주기는 독립적이다.

우선 병렬화 단위를 CombatRoom 사이로 잡는다. 하나의 CombatRoom 내부를 여러 스레드가
동시에 수정하지 않는다.

### 4.7 Shared Content Runtime

여러 유저가 공유하는 이벤트 기반 상태를 별도 Actor가 소유한다.

- PartyActor: 파티 구성, 파티장, 참가 상태
- GuildActor: 길드원, 역할, 길드 재화
- MatchmakingActor: 대기열과 매칭 결과
- ChatChannelActor: 채널 참가 상태

개인 인벤토리, 퀘스트, 상점 구매처럼 한 유저가 소유하는 기능은 Shared Content Runtime에
넣지 않고 PlayerActor의 handler 또는 module로 둔다. `ContentRuntime`이 소유자가 불분명한
기능을 모으는 공간이 되지 않게 한다.

### 4.8 Persistence Runtime

게임 Worker에서 동기 DB 호출을 하지 않는다.

```text
PlayerActor
→ DbRequest
→ bounded DB Worker Pool
→ blocking DB operation
→ DbCompleted
→ 원래 PlayerActor Worker
```

DB Worker는 PlayerActor 상태를 직접 수정하지 않는다. 완료 메시지에는 최소한 다음 정보를
포함한다.

- request ID
- entity ID
- 요청 시점의 entity version
- 성공 또는 실패 결과
- 재시도 가능 여부

PlayerActor는 완료 메시지를 자신의 mailbox 또는 Worker queue에서 처리하며 오래된 결과가
최신 상태를 덮지 못하게 한다.

권장 저장 전략은 다음과 같다.

- 로그인 시 PlayerActor 상태 로드
- 실행 중 메모리 상태를 authoritative state로 사용
- dirty 상태의 주기적인 비동기 flush
- 로그아웃 시 snapshot 저장
- `battle_id`를 이용한 전투 결과 멱등 처리
- 재화 같은 중요 변경에 명시적인 실패 및 재시도 정책 적용

### 4.9 Observability

로그 기록은 전용 bounded queue와 Logger Thread에서 처리한다. 일반 로그가 가득 찼을 때
게임 Worker를 무기한 막지 않도록 sampling 또는 drop 정책을 정의한다. 감사나 결제 성격의
로그는 별도의 신뢰성 정책을 가져야 한다.

최소 측정 항목은 다음과 같다.

- Reactor event loop 지연
- 연결 수와 Session별 송신 queue 크기
- Runtime별 queue depth
- command enqueue부터 실행까지의 p50/p95/p99 대기 시간
- World 및 Battle tick 실행 시간
- tick budget 초과 횟수
- 활성 Zone, CombatRoom, PlayerActor 수
- DB queue depth와 query latency
- dropped, rejected, coalesced message 수

## 5. 패킷 처리 흐름

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant N as Network Reactor
    participant G as Protocol Gateway
    participant R as Command Router
    participant P as Player Worker
    participant W as World Worker
    participant B as Battle Worker
    participant D as DB Worker

    C->>N: TCP bytes
    N->>N: recv · frame decode · 최소 검증
    N->>G: FrameEnvelope(connectionId, frame)
    G->>G: dispatch · payload validation · route snapshot
    G->>R: RoutedCommand(target, routeEpoch, command)

    alt 유저 영구 상태 명령
        R->>P: post(playerId, command)
        P->>P: PlayerActor가 순차 처리
        opt DB 작업 필요
            P->>D: DbRequest
            D-->>P: DbCompleted
            P->>P: version 확인 후 완료 처리
        end
        P-->>N: OutboundMessage
    else 월드 입력
        R->>W: post(zoneId, command)
        W->>W: 다음 World tick에서 이동·충돌·AOI
        W-->>N: OutboundMessage
    else 전투 입력
        R->>B: post(roomId, command)
        B->>B: 다음 Battle tick에서 이동·충돌·공격
        B-->>N: OutboundMessage
    end

    N->>N: generation 확인 · frame encode
    N-->>C: TCP bytes
```

이동이나 공격 패킷은 Network Thread에서 즉시 시뮬레이션하지 않는다. 입력 command를
해당 Zone 또는 CombatRoom에 넣고 다음 권위 있는 tick에서 처리한다.

## 6. 월드와 전투 사이 상태 전환

```mermaid
sequenceDiagram
    autonumber
    participant W as Zone
    participant P as PlayerActor
    participant B as CombatRoom
    participant R as Command Router
    participant D as DB Worker

    W->>W: 필드 충돌로 전투 진입 감지
    W->>P: BeginBattle(encounterId)
    P->>P: 전투 가능 여부 검증
    P->>P: 상태를 InBattle(roomId)로 전환
    P->>B: CreateCombatant(snapshot)
    B->>B: CombatRoom 상태 생성
    B->>R: UpdateRoute(Battle, roomId)

    Note over B: 이후 전투 입력은<br/>CombatRoom으로 직접 전달

    B->>B: fixed tick으로 전투 처리
    B->>P: BattleResult(battleId, result)
    P->>P: 결과를 멱등하게 적용
    P->>D: 비동기 저장
    P->>W: ReturnToWorld
    W->>R: UpdateRoute(World, zoneId)
```

전투 중 임시 HP와 위치는 CombatRoom이 소유한다. PlayerActor는 전투 참여 상태와 영구
데이터를 소유하고 전투가 끝난 뒤 BattleResult를 적용한다. 두 객체가 같은 전투 상태를
동시에 수정하지 않는다.

구현 시 route 변경은 `RouteCoordinator`가 직렬화한다.

```text
이전 destination 입력 중지와 drain 경계 확정
→ 새 destination activation 완료
→ SessionRoute target과 epoch 갱신
→ 새 epoch command 공개
```

destination은 command의 epoch을 자신의 activation epoch과 비교해 전환 전에 queue에 들어간 stale
입력을 거부한다. 동일 epoch 안의 중복과 역순은 별도 sequence/idempotency 규칙으로 처리한다.

## 7. 메시지 규약

Runtime 경계를 넘는 메시지는 immutable value 또는 명확한 단독 소유 값을 사용한다.
다른 Runtime이 소유한 객체의 raw pointer나 mutable reference를 전달하지 않는다.

공통 envelope는 다음 정보를 가질 수 있다.

```cpp
struct MessageEnvelope
{
    MessageId message_id;
    CorrelationId correlation_id;
    EntityId target_id;
    std::optional<ConnectionId> connection_id;
    std::optional<std::uint64_t> route_epoch;
    std::optional<std::uint64_t> sequence;
    TimePoint deadline;
    MessagePayload payload;
};
```

모든 메시지에 모든 필드를 강제할 필요는 없지만 다음 의미는 일관되게 유지한다.

- `message_id`: 중복 처리 방지
- `correlation_id`: 요청, DB 작업, 응답 연결
- `target_id`: shard와 상태 소유자 선택
- `connection_id`: 응답 Session 수명 검증
- `route_epoch`: 이전 destination으로 향한 command 검출
- `sequence`: 동일 route 스트림의 중복·역순 검증
- `deadline`: 이미 가치가 없어진 작업 폐기

Runtime 사이에서 동기 호출이나 `future.get()`으로 상대 Worker를 기다리지 않는다.

## 8. Queue와 Backpressure

모든 비동기 queue는 bounded queue여야 한다. 큐가 가득 찼을 때의 정책을 호출 지점별로
정의한다.

| 포화 지점 | 권장 정책 |
| --- | --- |
| Session inbound | 읽기 일시 중지, rate limit, 심한 경우 연결 종료 |
| Player command queue | Busy 응답 또는 비핵심 명령 거부 |
| World/Battle input queue | 최신 입력 병합, 오래된 입력 폐기, 악성 세션 종료 |
| Reactor outbound queue | 상태 갱신 병합 또는 slow client 종료 |
| DB queue | 제한된 재시도 또는 요청 실패 처리 |
| 일반 로그 queue | sampling 또는 drop |

입력 종류별로 정책이 다를 수 있다. 이동 입력은 최신 값으로 합칠 수 있지만 아이템 구매나
전투 결과는 임의로 버리면 안 된다.

명령별 전달 의미와 포화 정책은 다음을 기본값으로 삼는다.

| 종류 | 전달 의미 | 포화 시 정책 |
| --- | --- | --- |
| 이동 입력 | 최신 상태 우선 | 최신 입력으로 coalesce/replace |
| 일반 공격 | route 내 sequence 적용 | stale 폐기 또는 Busy |
| 구매·보상 | effect를 한 번만 적용 | 명시적 거부, idempotency key와 transaction |
| `BattleResult` | 임의 유실 금지 | reserved capacity와 retry, persistence 이후 durable handoff 검토 |
| 악성 요청 | 서비스 보호 우선 | rate limit 또는 연결 종료 |

구매·보상은 exactly-once delivery를 가정하지 않는다. at-least-once 재전달 가능성을
idempotency와 transaction으로 흡수해 effectively-once application을 만든다.

## 9. Tick 설계

World와 Battle은 서로 독립된 fixed tick 주기를 가질 수 있다. 초기 예시는 World 20Hz,
Battle 30Hz지만 실제 값은 게임 규칙과 부하 테스트로 결정한다.

고정 tick의 기본 규칙은 다음과 같다.

- 시간 계산에는 `std::chrono::steady_clock`을 사용한다.
- 테스트에서는 주입 가능한 Clock을 사용한다.
- 한 tick이 늦어져도 무제한 catch-up loop를 돌지 않는다.
- tick당 command 처리량 또는 시간을 제한한다.
- tick 실행 시간이 budget을 넘으면 metric과 structured log를 남긴다.
- 네트워크 snapshot 주기는 simulation tick과 별도로 설정할 수 있다.

결정론적인 테스트를 위해 동일한 초기 상태와 입력 sequence가 같은 결과를 내도록 하는 것을
권장한다.

## 10. Thread 토폴로지

초기 구현은 다음과 같이 시작한다.

| 영역 | 초기 Worker 수 | 확장 기준 |
| --- | ---: | --- |
| Network Reactor | 1 | event loop 지연과 네트워크 처리량 |
| Player Actor | 1 | mailbox 대기 시간 |
| World | 1 | tick budget과 Zone 수 |
| Battle | 1 | tick budget과 활성 Room 수 |
| Shared Content | 1 | Actor별 queue 지연 |
| DB | 2 | DB connection 한도와 queue 지연 |
| Logger | 1 | 로그 queue 지연 |

최종 학습 단계에서는 World 또는 Battle Worker를 최소 2개로 늘려 다음을 검증한다.

- 동일 Zone 또는 Room은 한 Worker에서만 실행된다.
- 서로 다른 shard는 실제로 병렬 실행된다.
- 특정 shard의 부하가 다른 shard의 순서를 깨뜨리지 않는다.
- worker_count 변경과 entity 재배치 정책이 명확하다.

CPU를 지속적으로 사용하는 Worker 수는 물리 코어 수와 tick deadline을 기준으로 결정한다.
DB Worker처럼 대부분 대기하는 Thread는 같은 방식으로 단순 합산하지 않는다.

## 11. 종료 순서

graceful shutdown은 다음 순서를 권장한다.

```text
새 연결 수락 중지
→ Session의 새 게임 command 수락 중지
→ World/Battle에 종료 경계 전달
→ 게임 Runtime queue drain
→ RuntimeCompletionCoordinator가 모든 필수 Runtime drain 확인
→ 필요한 dirty state 저장 요청
→ DB queue drain 또는 timeout
→ outbound queue drain 또는 timeout
→ Reactor 종료
→ Logger flush
```

runtime 완료 상태는 outbound queue와 분리한다. coordinator의 상태가 authoritative하며 wake-up은
상태 재조회를 촉진하는 hint다. 종료 중에도 다른 Runtime이 이미 파괴된 Session이나 Actor에
메시지를 보내지 않도록 Runtime 간 수명 순서를 명시해야 한다.

## 12. 목표 소스 구조

```text
include/snf/
├── core/
│   ├── id.hpp
│   ├── message.hpp
│   ├── clock.hpp
│   └── result.hpp
├── net/
│   ├── network_runtime.hpp
│   ├── epoll_reactor.hpp
│   ├── reactor_group.hpp
│   ├── session.hpp
│   └── connection_id.hpp
├── gateway/
│   ├── command_router.hpp
│   └── session_route.hpp
├── runtime/
│   ├── bounded_queue.hpp
│   ├── event_executor.hpp
│   ├── fixed_tick_executor.hpp
│   └── timer_scheduler.hpp
├── game/
│   ├── player/
│   ├── world/
│   ├── battle/
│   └── social/
├── persistence/
│   ├── db_executor.hpp
│   ├── player_repository.hpp
│   └── persistence_message.hpp
├── protocol/
└── observability/
    ├── async_logger.hpp
    └── metrics.hpp
```

이 구조는 목표 형태이며 파일을 먼저 모두 만들 필요는 없다. 실제 기능을 구현할 때 필요한
경계를 순서대로 추가한다.

## 13. 현재 구조에서의 발전 단계

실행 순서는 [개발 로드맵](./development-roadmap.md)과 동일하다.

### 단계 1~3: Network 분리, Sharded ActorRuntime, PlayerActor/Mailbox (완료)

```text
epoll Reactor
→ FrameIngress
→ ProtocolGateway(MessageDispatcher)
→ RoutedCommand
→ CommandRouter
→ ActorRuntime shard ingress
→ PlayerActor mailbox와 ready queue
→ typed PlayerResult
→ PlayerEffectSink
→ ProtocolResponseMapper
→ OutboundSink
→ Reactor send
```

동일 Actor FIFO·단일 실행, shard 병렬성, fairness budget, backpressure, drain/cancel/실패와
stale `ConnectionId`를 검증했다.

### 단계 3.5: Coroutine 계약 준비와 경계 강화 (완료)

- `ConnectionId`를 net 계층으로 이동하고 연결 기반 키를 `ProvisionalActorId`로 명확히 했다.
- ActorRuntime에서 raw outbound queue와 `eventfd`, protocol `Frame` 의존을 제거했다.
- `PlayerResult → PlayerEffectSink → ProtocolResponseMapper → OutboundSink` pipeline을 만들고
  handler 예외 전에는 effect가 방출되지 않도록 했다.
- `ConnectionClosed`를 기존 ingress FIFO로 전달한다. reactor는 `Full` lifecycle post를 별도 pending
  deque에서 budgeted retry하며, owning worker가 close를 소비한 turn 끝에서 actor를 evict한다. active
  session과 pending close는 bounded lifecycle slot 예산을 공유하고, 소진 시 새 연결을 Actor 생성 전에
  거부한다.
- `OutboundAction`과 runtime completion 제어 경로를 분리했다.
- `RuntimeCompletionCoordinator`가 required runtime mask의 drain/failure를 authoritative state로
  추적한다.
- Coroutine continuation, frame 수명과 shutdown 규약은
  [Coroutine Actor 계약](./coroutine-actor-contract.md)에 고정했다.

### 단계 4: Coroutine Actor

- `ActorTask<PlayerResult>`, bounded continuation reservation과 Worker-affine resume을 구현한다.
- suspend된 Actor의 command는 mailbox에서 기다리고 다른 Actor는 진행한다.
- cancel, late completion과 drain 경합을 Debug, sanitizer와 deterministic test로 검증한다.

### 단계 5: 인증·영속성 Vertical Slice

- 영속 `PlayerId`, 비동기 load, 멱등한 구매와 transaction 저장을 구현한다.
- disconnect/passivation/reconnect 복원까지 하나의 흐름으로 검증한다.

### 단계 6: 최소 World와 Battle

- FixedTickExecutor, Zone과 CombatRoom의 최소 이동·충돌·전투 흐름을 구현한다.
- RouteCoordinator, route epoch, multi-runtime drain과 `BattleResult` 멱등 적용을 검증한다.

### 단계 7: Shared Content와 Projection

- Party 또는 Matchmaking 하나를 구현한다.
- 랭킹과 시즌 정산은 domain event 기반 projection과 재실행 가능한 job으로 분리한다.

### 선택적 인프라 트랙: io_uring과 프로세스 분리

- 안정된 inbound/outbound/lifecycle 계약을 두 번째 network backend로 검증할 때 io_uring을
  추가한다.
- 단일 프로세스 경계를 충분히 검증한 뒤에만 Gateway, WorldNode, BattleNode 분리를 실험한다.
- 그 전에는 외부 message broker를 hot packet path에 두지 않는다.

## 14. 검증 기준

최종 학습 구조는 다음 조건을 자동화된 테스트와 부하 테스트로 검증해야 한다.

- 동일 PlayerActor의 handler가 동시에 실행되지 않는다.
- 동일 Zone과 CombatRoom을 여러 Worker가 동시에 수정하지 않는다.
- 서로 다른 shard의 Actor 또는 Room은 병렬 실행된다.
- 네트워크 Reactor가 게임 로직, DB 또는 파일 I/O 때문에 멈추지 않는다.
- 같은 Session의 명령 순서가 필요한 범위에서 보장된다.
- 재접속 후 오래된 generation의 응답이 새 Session으로 전달되지 않는다.
- World와 Battle tick이 독립적으로 유지된다.
- tick overrun을 감지하고 수치로 확인할 수 있다.
- queue가 포화되었을 때 정의된 backpressure가 동작한다.
- DB 완료 순서가 바뀌어도 최신 상태가 과거 결과로 덮이지 않는다.
- 같은 BattleResult가 중복 전달되어도 한 번만 적용된다.
- shutdown 시 새 작업 차단, queue drain, 저장, 송신 drain이 순서대로 실행된다.
- 부하 테스트에서 queue depth, command wait, tick time과 end-to-end p99를 확인할 수 있다.

## 15. 의도적으로 미루는 항목

다음 항목은 핵심 구조를 검증하기 전에는 도입하지 않는다.

- Actor 하나당 Thread
- 모든 객체를 Actor로 만드는 설계
- 하나의 CombatRoom 내부 병렬 업데이트
- 측정 없는 lock-free 자료구조 전환
- 모든 비동기 흐름의 coroutine 전환
- 초기 단계의 microservice 또는 외부 message broker
- 영구적인 global singleton 상태
- 여러 Runtime이 같은 게임 상태를 mutex로 공동 수정하는 구조

## 16. 미결정 사항

다음 항목은 게임 규칙과 측정 결과에 따라 별도로 결정한다.

- World 및 Battle tick rate
- 한 Worker가 소유할 Zone과 CombatRoom 상한
- hot Zone의 공간 분할 방식
- Actor별 mailbox 도입 시점과 batch budget
- DB 종류, connection pool 크기와 저장 보장 수준
- snapshot, event journal 또는 outbox 도입 범위
- 네트워크 패킷 sequence와 재전송 정책
- 프로세스 분리 시 내부 프로토콜과 Actor Directory 방식

---

SnF의 목표 아키텍처를 한 문장으로 정리하면 다음과 같다.

> `epoll` Reactor Group, keyed Player Actor Shard, fixed-tick Zone/Battle Shard,
> bounded 비동기 persistence와 명확한 상태 소유권을 가진 분리 가능한 모듈러 게임 서버.
