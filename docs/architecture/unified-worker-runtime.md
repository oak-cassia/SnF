# Unified Worker Runtime 아키텍처

> 상태: **구현 목표 / 저장소의 런타임 아키텍처 기준 문서**
> 기준 버전: 사용자 제공 `Unified Worker Runtime 상세 설계서 v1.3` (2026-08-29)
> 적용 범위: C++ 게임 서버의 connection I/O, Actor 실행, timer, 비동기 DB, effect 적용과 lifecycle
> 주의: 이 문서는 목표 구조를 설명한다. 현재 코드 구조는 전환이 끝날 때까지 다를 수 있다.

현재 구현은 개발 로드맵 6단계까지 반영한다. `snf::worker::WorkerGroup`이 `SO_REUSEPORT` listener와
Worker를 bootstrap하고, Worker는 bounded connection I/O와 close lifecycle, ActorTable·mailbox·ready
queue, synthetic blocked operation과 activation, concrete effect 적용, local/cross-worker tell 및
publication-safe shutdown barrier를 소유한다. Domain result adapter와 native async DB는 이후 전환
단계의 대상이다.

설계의 중심 문장은 다음과 같다.

> 하나의 mutable state에는 하나의 owner Worker만 존재한다. owner가 다르면 message로 전달하고,
> 비동기 continuation은 시작한 owner Worker에서만 재개한다.

## 1. 최종 구조

```text
Server
├─ WorkerPool
│  ├─ Worker 0
│  ├─ Worker 1
│  ├─ ...
│  └─ Worker N
└─ BlockingAdapterExecutor       // 동기 API가 남아 있을 때만 사용하는 선택 경로
```

```text
Worker = 1 OS thread
│
├─ Poller
│  ├─ client sockets
│  ├─ native async DB sockets
│  └─ wakeup handle
├─ ConnectionTable
├─ ActorTable + ReadyActorQueue
├─ WorkerInbox
├─ TimerQueue
└─ Worker-local DbClient         // DB를 사용하는 Worker에만 존재
```

`Worker`는 모든 코드를 한 클래스에 넣는 God object가 아니다. table, queue, protocol helper와
translation unit은 분리할 수 있다. 다만 호출 thread와 mutable state owner는 Worker 하나로 고정한다.

핵심 개념은 다섯 개뿐이다.

| 개념 | 책임 |
| --- | --- |
| `Worker` | mutable state와 continuation의 단일 owner, event loop 실행 |
| `ActorSlot` | Actor domain state, mailbox, lifecycle과 현재 blocked operation 보관 |
| `WorkerInbox` | 다른 Worker 또는 foreign thread에서 들어오는 단일 논리 ingress |
| `Effect` | domain result를 infrastructure action으로 표현한 ordered command |
| `AwaitKey` | completion을 특정 Actor activation과 operation에 연결하는 identity |

## 2. Core runtime에 두지 않는 구조

| 기존 또는 후보 구조 | 결정 |
| --- | --- |
| 별도 `OutcomeHandler` 계층 | 만들지 않는다. `toEffects(context, result)` 함수 또는 overload로 표현한다. |
| `PendingOperationTable` | 제거한다. continuation, deadline과 completion identity의 기준점은 `ActorSlot.blocked` 하나다. |
| `CompletionDispatcher` | 제거한다. native DB는 `Worker::completeDb()`, 선택 adapter는 구체 completion 함수로 수렴한다. |
| generic `AsyncGateway` | MVP에서 만들지 않는다. 구체 `DbClient` API를 사용한다. |
| `OutboundQueue` / `OutboundSink` | core runtime에서 제거한다. connection owner가 `ConnectionSlot.writeBuffer`에 직접 append한다. |
| `WorkflowTable` | 만들지 않는다. 먼저 domain Actor의 명시적 message-driven state로 표현한다. |
| catch-all `ControlEvent` | 만들지 않는다. stop flag와 wakeup, 실제 요구별 concrete event를 사용한다. |
| Actor migration / reentrancy | v1.3 범위 밖이다. owner는 고정하고 Actor는 non-reentrant다. |
| `BlockingAdapterExecutor` | 동기 API가 남아 있을 때만 활성화한다. native async DB의 기본 경로가 아니다. |

`StartDetachedEffect`, `CancelTimerEffect`, 범용 effect failure-policy enum과 모든 backend를 포괄하는
async interface도 MVP에 넣지 않는다. 실제 두 번째 요구가 생긴 뒤 공통 계약을 추출한다.

## 3. 핵심 불변식

| ID | 불변식 |
| --- | --- |
| INV-01 | Actor, Connection과 Worker-local DB connection의 mutable state는 owner Worker만 접근한다. |
| INV-02 | owner가 다르면 직접 호출하지 않고 message/event로 전달한다. |
| INV-03 | local tell도 mailbox를 통과하며 Actor handler를 inline 호출하지 않는다. |
| INV-04 | Actor는 non-reentrant다. blocked operation 동안 다음 mailbox command를 실행하지 않는다. |
| INV-05 | 비동기 continuation은 시작한 owner Worker에서만 재개한다. |
| INV-06 | continuation, deadline과 completion identity의 canonical owner는 `ActorSlot.blocked`다. |
| INV-07 | DB client와 executor는 protocol/job 진행 상태만 소유하고 Actor continuation을 소유하지 않는다. |
| INV-08 | timer, completion과 delayed send는 incarnation, generation과 operation ID를 검증한다. |
| INV-09 | 모든 queue, buffer, table과 backend admission은 bounded이며 cross-thread enqueue는 block하지 않는다. |
| INV-10 | Worker thread에서 잠재적으로 blocking인 외부 API를 호출하지 않는다. |
| INV-11 | event-loop phase는 count, byte 또는 time budget을 가진다. |
| INV-12 | Worker는 domain result 의미를 해석하지 않고 ordered `EffectBatch`만 적용한다. |
| INV-13 | I/O와 callback phase는 coroutine을 inline resume하지 않고 ready queue에 resume turn을 예약한다. |

## 4. Ownership와 배치

Actor owner는 시작 시 고정된 Worker 수로 계산한다.

```cpp
WorkerId ownerOf(ActorKey key) {
    return stableHash(key.type, key.id, placement_seed) % worker_count;
}
```

Actor reference에는 owner를 중복 저장하지 않는다. Connection owner는 accept 시 결정되어 ID만으로
유도할 수 없으므로 reference에 포함한다.

```cpp
struct ConnectionRef {
    ConnectionId id;
    ConnectionGeneration generation;
    WorkerId owner;
};
```

DB connection은 생성한 Worker가 수명 전체를 소유한다. 여러 Worker가 하나의 DB connection을 mutex로
공유하지 않는다.

```text
현재 Worker가 owner
-> state 직접 접근

다른 Worker가 owner
-> WorkerEvent enqueue

executor/callback thread
-> immutable completion을 owner WorkerInbox에 enqueue
```

다른 Worker나 executor는 `Actor*`, `ActorSlot*`, `ConnectionSlot*`, coroutine handle 또는 owner
allocator에 종속된 view를 보관하지 않는다.

## 5. 핵심 데이터 모델

```cpp
struct ActorKey {
    ActorType type;
    uint64_t id;
};

struct ActivationRef {
    ActorKey key;
    ActorIncarnation incarnation;
};

struct AwaitKey {
    ActorKey actor;
    ActorIncarnation incarnation;
    OperationId operation_id;
};
```

`OperationId`는 같은 Actor incarnation에서 재사용하지 않는다. `AwaitKey`에는 routing, deadline,
retry policy나 result payload를 넣지 않는다.

다른 Worker 또는 foreign thread의 event는 하나의 논리 ingress로 통합한다.

```cpp
using WorkerEvent = std::variant<
    RemoteActorMessage,
    RemoteConnectionSend,
    RemoteConnectionClose,
    BlockingJobCompleted>;       // adapter를 사용할 때만 포함
```

Native DB completion은 같은 Worker의 poll phase에서 발생하므로 inbox를 거치지 않고
`Worker::completeDb()`로 들어온다. Blocking adapter job은 submit 시 completion slot까지 예약해
accepted completion이 queue full로 유실되지 않게 한다.

6단계의 armed shutdown barrier에서는 cross-worker event producer를 같은 barrier에 참가하는 Worker
thread로 제한한다. foreign-thread producer를 실제로 추가할 때는 producer participation counter 또는
별도 in-flight publication protocol을 함께 도입해야 한다.

현재 concrete event는 `RemoteActorMessage`, `RemoteConnectionSend`, `RemoteConnectionClose`다.
connection event는 full `ConnectionRef`를 운반하며 대상 Worker가 generation을 검증한 뒤 owner-local
`ConnectionSlot`에 적용한다. Actor event는 target `ActorKey`를 재검증한 뒤 local mailbox에 admission한다.

```cpp
struct ActivationLoad {
    AwaitKey key;
    TimePoint deadline;
};

struct SuspendedDbCommand {
    AwaitKey key;
    TimePoint deadline;
    CoroutineTask continuation;
    std::optional<DbResult> completion;
};

struct SuspendedBlockingJob {    // optional
    AwaitKey key;
    TimePoint deadline;
    CoroutineTask continuation;
    std::optional<BlockingJobResult> completion;
};

using BlockedTask = std::variant<
    ActivationLoad,
    SuspendedDbCommand,
    SuspendedBlockingJob>;

struct ActorSlot {
    ActorKey key;
    ActorIncarnation incarnation;
    std::optional<Actor> actor;   // Loading 동안 비어 있을 수 있음
    Mailbox mailbox;
    ActorState state;
    std::optional<BlockedTask> blocked;
};
```

한 Actor에는 `BlockedTask`가 최대 하나만 존재한다.

```text
ActorSlot.blocked
- continuation 또는 activation load state
- logical deadline
- completion identity
- command에 전달할 completion

DbClient::in_flight
- DB connection 배정
- partial read/write와 protocol decode
- physical cancel 진행

TimerQueue
- deadline에 AwaitTimeout{AwaitKey}를 찾기 위한 scheduling index
```

TimerQueue의 deadline copy는 검색용이다. logical timeout의 유효성은 언제나 현재
`ActorSlot.blocked`와 `AwaitKey`가 일치하는지로 판정한다.

## 6. Worker event loop

논리 phase는 다음 순서를 반복한다.

```text
1. Poll I/O
2. Drain WorkerInbox
3. Expire timers
4. Run ready Actors
5. Flush connection writes
```

DB socket readiness는 1번, blocking adapter completion은 2번, awaited timeout은 3번에 속한다.
처리할 일이 남으면 다음 poll timeout을 0으로 만들되, 어느 phase도 queue 전체를 무제한 drain하지 않는다.

초기 budget은 다음 값에서 부하 테스트로 조정한다.

| Phase | 초기 상한 |
| --- | ---: |
| Poll dispatch | 1,024 poll events |
| Accept | 64 accepts |
| Read | 1,024 connections, 1,024 decoded frames, 4 MiB |
| WorkerInbox | 4,096 events 또는 250 μs |
| Timer expiry | 2,048 entries 또는 250 μs |
| Actor turns | 1,024 turns 또는 1 ms |
| Write flush | 4 MiB 또는 500 μs |

Poll dispatch, accept와 read는 하나의 I/O phase duration인 500 μs를 공유한다. 각 sub-phase의 count/byte
상한은 서로 독립적이며, `max_poll_events`, `max_accepts`, `max_read_connections`, `max_frames`와
`max_read_bytes`로 표현한다.

I/O, inbox와 timer phase는 Actor 코드를 직접 실행하지 않는다.

## 7. Actor 실행 모델

```text
Loading -> Idle/Queued/Stopping
Idle -> Queued
Queued -> Running
Running -> Idle/Queued
Running -> Suspended
Suspended -> Queued
Idle -> Stopping
Stopping -> Removed
```

| 상태 | 의미 | mailbox command 실행 |
| --- | --- | --- |
| Loading | `ActivationLoad` 완료 대기. message는 mailbox에 축적 | 금지 |
| Idle | 활성 상태이며 mailbox가 비어 있음 | 해당 없음 |
| Queued | `ReadyActorQueue`에 정확히 한 번 등록 | 대기 |
| Running | 새 command 하나 또는 resume turn 하나 실행 | 한 turn |
| Suspended | 현재 blocked command 완료 대기 | 금지 |
| Stopping | shutdown, fatal 또는 explicit stop 처리 | 정책에 따라 거부 |

`Queued` 상태가 ready queue 중복 삽입을 방지한다. `Loading`과 `Suspended`에서는 mailbox에만
축적한다. 새 slot을 만들기 전에 ActorTable hard cap, concurrent Loading cap, mailbox와 activation DB
admission을 모두 확인한다.

한 turn에서는 다음 중 하나만 실행한다.

- 완료된 continuation 하나를 resume
- mailbox의 새 command 하나를 dispatch

turn이 DB에서 suspend되면 `SuspendedDbCommand`를 `ActorSlot.blocked`에 저장하고 await timeout을
예약한다. completion은 slot을 `Queued`로 전환할 뿐이며 실제 resume는 다음 Actor phase에서 수행한다.

Local과 remote tell의 공개 API는 하나다. local tell도 mailbox를 거쳐 fairness, failure isolation,
bounded stack과 non-reentrancy를 보존한다.

Activation은 일반 suspended command와 구분한다.

```text
missing Actor에 message 도착
-> ActorTable/Loading capacity 확인
-> 새 incarnation의 Loading slot 생성
-> slot.blocked = ActivationLoad
-> native async DB load 시작
-> message는 mailbox에 저장
-> load 성공: actor 생성 후 Idle 또는 Queued
-> load 실패: queued message를 activation failure로 종료하고 slot 제거
```

MVP에는 idle passivation을 넣지 않는다. 실제 메모리 압력이 확인된 뒤 `Idle + mailbox empty + blocked
없음`을 조건으로 추가한다.

## 8. Connection I/O

```text
socket readable
-> owner Worker
-> ReadBuffer append
-> frame decode와 request validation/translation
-> Actor message enqueue
```

초기에는 protocol validation과 use-case routing을 하나의 `RequestHandler`로 구현해도 된다. 독립 변경
이유가 생길 때만 Gateway와 Service로 나눈다.

별도 outbound hop은 두지 않는다.

```text
local send
-> ConnectionId + Generation 검증
-> owner ConnectionSlot.writeBuffer에 append

remote send
-> connection owner WorkerInbox에 RemoteConnectionSend
-> owner가 검증 후 writeBuffer에 append
```

`Worker::send()`는 전달받은 rvalue `Frame`을 성공/실패와 관계없이 소비하며 실패한 frame을 호출자에게
돌려주지 않는다. Inbox의 `tryPush()`는 자체 admission check가 실패하면 전달받은 `WorkerEnvelope`를
move하지 않지만, 이미 envelope가 소유한 frame은 `Worker::send()`의 terminal 결과와 함께 폐기된다.

`ConnectionTable`과 `PollRegistrationTable`은 서로 독립적인 bounded table이다. `accept4` 이후에는
connection reservation, poll-registration reservation, `epoll_ctl(ADD)`를 모두 성공시킨 뒤 commit한다.
중간 실패는 RAII rollback으로 socket, registration과 connection slot을 함께 반환한다. 두 table 중
하나라도 여유가 없으면 listener의 read interest를 끄고, 둘 다 여유가 생긴 뒤에만 다시 켠다.

Read와 write work queue에는 pointer나 descriptor가 아니라 `{ConnectionId, ConnectionGeneration}`만
저장한다. queue item을 꺼낼 때 full 64-bit generation을 다시 조회하므로 slot이 닫힌 뒤 재사용되어도
stale work가 새 connection에 적용되지 않는다. connection당 `read_queued`, `write_queued` bit가
중복 등록을 막는다.

Write queue는 encoded bytes를 보관한다. 새 frame은 `!write_queued && !waiting_epollout`일 때만
queue에 넣고, `send()`가 EAGAIN이면 queue item을 제거한 뒤 `waiting_epollout`과 EPOLLOUT만 남긴다.
EPOLLOUT event는 full generation을 확인하고 EPOLLOUT를 끈 뒤 한 번만 다시 queue에 넣는다. 같은
poll batch의 중복 writable event는 이미 지워진 `waiting_epollout` bit로 무시한다.

Read path의 `RequestSink`는 request를 정확히 한 번 소비한다. `Accepted`는 책임 인수,
`Invalid`는 protocol violation 즉시 close, `Rejected`는 overload 즉시 close이며 request retry나
pending request state는 없다.

Connection 상태는 `Open -> Closing -> Closed`다. Closing에서는 신규 application send를 거부하고
drain deadline까지 기존 write buffer를 flush한다.

초기 buffer 기준은 다음과 같다.

| 항목 | 기준 | 초과 처리 |
| --- | ---: | --- |
| Body | 65,536 bytes | protocol violation close |
| Payload | 65,530 bytes | protocol violation close |
| 전체 frame | 65,540 bytes | protocol violation close |
| ReadBuffer | 512 KiB | 완전한 frame을 만들지 못하면 close |
| Write soft watermark | 1 MiB | noncritical send를 append 전에 SoftLimit로 거부 |
| Write hard watermark | 4 MiB | slow consumer close |
| Close drain deadline | 2 s | 강제 close |

Buffer 기준은 logical cap이다. connection마다 최대 read/frame 크기를 미리 reserve하지 않으며,
decoder의 현재 buffered bytes와 write queue의 실제 queued bytes만 admission에 반영한다.

Connection은 `Open -> Closing -> Closed`로 진행한다. Closing 진입 시 EPOLLIN과 application read를
즉시 끄고 decoder를 reset한다. 기존 write buffer만 deadline까지 drain하며, 빈 buffer면 즉시 닫는다.
protocol violation, slow consumer, peer close와 I/O error는 drain 없이 즉시 close한다.

## 9. Domain Result와 Effect

Worker는 `PlayerResult`, `ZoneResult`, `RoomResult`를 해석하지 않는다. mapping은 runtime 계층이 아니라
순수 변환 책임이다.

```cpp
EffectBatch toEffects(
    const RoomOutcomeContext& context,
    RoomResult&& result);
```

```cpp
using Effect = std::variant<
    SendFrameEffect,
    CloseConnectionEffect,
    TellActorEffect,
    ScheduleTimerEffect,
    StopActorEffect>;

class EffectBatch {
public:
    static constexpr std::size_t MAX_EFFECTS = 64;
    bool tryPush(Effect effect); // MAX_EFFECTS에서 명시적으로 reject

private:
    std::vector<Effect> ordered;
};
```

Effect를 종류별 vector로 나누지 않고 한 ordered list에 둔다. 이미 적용한 앞 effect는 뒤 effect 실패로
rollback하지 않는다. 각 effect의 실패 의미는 concrete type 계약으로 고정한다.

`Effect`, `EffectBatch`, `CompletedTurn`은 copy하지 않고 move-only chain으로 Worker까지 전달한다.
`MAX_EFFECTS`는 컨테이너의 inline capacity가 아니라 명시적인 runtime hard cap이다. 현재 Room의 참가자
hard cap 4명에서 계산한 domain upper bound는 16이며 `static_assert(16 <= 64)`로 연결한다.

| Effect | 실패 의미 |
| --- | --- |
| SendFrame | soft limit에서는 reject/coalesce, hard limit에서는 connection close 정책 |
| CloseConnection | owner에서 generation 검증 후 idempotent Closing; stale/closed는 no-op |
| TellActor | bounded delivery. 실패는 metric과 diagnostic에 기록; 강한 보장은 별도 protocol로 모델링 |
| ScheduleTimer | bounded insert 실패 시 runtime failure를 기록하고 기본적으로 남은 batch 중단 |
| StopActor | safe point에서 idempotent Stopping 전환 |

한 batch에는 `StopActorEffect`와 새로운 `ScheduleTimerEffect`가 함께 존재하지 않는다. 7단계 application
timer는 현재 dispatch 중인 Actor 자신만 대상으로 하며 Worker가 현재 `ActivationRef`를 stamp한다.

Room battle deadline처럼 domain mutation에 필수인 timer는 adapter가 deadline envelope을 먼저 만들고
그 envelope의 logical charge로 `handle()` 전에 자리를 예약한다. 실패하면 `RuntimeOverloaded`를 반환하고
domain handle을 호출하지 않는다. 성공 후 domain mutation이 발생했다면 reserved timer commit까지는
recoverable failure 지점이 아니며 invariant 위반은 process fail-fast다. Reservation의 RAII 반환은 resource
leak 방지이지 domain transaction rollback이 아니다.

`TimerReservation`은 특정 Worker TimerQueue, entry 1개, 정확한 message charge와 생성 turn ID에 묶인
move-only token이다. ActorSlot, mailbox, coroutine frame 또는 다음 event-loop iteration으로 넘겨 보관하지
않는다. 정상 token의 reserved commit은 allocation/capacity 때문에 실패하지 않으며 queue, turn 또는 charge
mismatch는 invariant violation이다. `toEffects()`는 외부 service 조회·새 예약·domain mutation을 하지 않고,
이미 준비된 값과 reservation token만 소비하는 deterministic mapping이다.

Application payload는 game runtime의 단일 `GameActorPayloadRegistry`에 모두 등록한다. Registry는 모든
concrete payload의 non-zero tag uniqueness를 compile time에 검증하고, envelope extraction은 tag와 실제
type identity를 함께 확인한다. `chargedBytes()`는 owned dynamic capacity를 포함하는 overflow-safe,
conservative logical memory charge다.

저장처럼 여러 statement로 이루어진 mutation은 하나의 logical operation이다. 하나의 connection, 하나의
deadline, 하나의 `AwaitKey`, 하나의 completion으로 진행하고 중간 단계는 Actor에게 보이지 않는다. COMMIT
결과는 세 가지다.

| 결과 | 의미 |
| --- | --- |
| `Committed` | 서버가 COMMIT을 확인했다 |
| `FailedBeforeCommit` | COMMIT 이전에 실패했고 미커밋 변경이 확실히 폐기됐다 |
| `CommitOutcomeUnknown` | COMMIT 드라이버 호출을 시작한 뒤 확인을 받지 못했다 |

Unknown 구간은 전송 완료가 아니라 **드라이버 호출 시작**부터다. 어느 바이트까지 도달했는지 애플리케이션이
추정하지 않는다. Unknown은 지표로 올리고 자동 retry하지 않는다.

강한 일관성이 필요한 작업은 Actor aggregate state, DB transaction, idempotency key, request/ack 또는
실제 요구가 있는 durable outbox로 해결한다. `EffectBatch` 자체는 transaction이 아니다.

Domain 코드에는 하나의 거대한 `WorkerContext`를 service locator로 넘기지 않는다. command와 Actor에는
필요한 repository, clock 같은 작은 dependency만 명시적으로 전달한다.

```cpp
struct PlayerCommandContext {
    PlayerRepository& players;
    Clock& clock;
};

TurnResult dispatch(
    ActorSlot& slot,
    ActorEnvelope&& message,
    PlayerCommandContext& context);
```

다음 접근은 금지한다.

```text
Domain Actor -> Worker
Domain Actor -> ActorTable / ConnectionTable
Domain Actor -> send() / tell() 직접 호출
```

Domain은 typed result만 반환하고 infrastructure action은 `toEffects()`가 만든다.

## 10. 비동기 DB와 continuation

기본 경로는 Worker-local native async DB다.

```text
true non-blocking DB client
-> Worker Poller에서 network I/O처럼 진행

synchronous DB/file/legacy SDK
-> 선택 BlockingAdapterExecutor에서 실행
```

`co_await` 문법이 아니라 API가 호출 thread를 점유하는지로 경로를 구분한다.

DB submit 결과는 세 가지다.

| 결과 | 동작 |
| --- | --- |
| Rejected | suspend하지 않고 즉시 overload/validation 결과 반환 |
| CompletedInline | 현재 turn에서 결과 반환; blocked state를 만들지 않음 |
| Pending | timeout과 backend admission 확보 후 blocked state를 확정하고 Worker에 제어 반환 |

Pending 확정은 같은 Worker에서 다음 순서로 처리한다.

```text
1. OperationId 생성
2. TimerQueue timeout slot 예약
3. DB backend admission
4. Rejected/CompletedInline이면 timeout 예약 해제
5. Pending이면 ActorSlot.blocked 저장 후 Suspended 전환
```

DB completion은 backend-local in-flight state를 먼저 제거한 뒤 `Worker::completeDb(AwaitKey, DbResult)`로
들어간다. 함수는 ActorKey, incarnation, operation ID와 concrete blocked type을 검증한다.

- `ActivationLoad`이면 Actor를 만들고 `Loading -> Idle/Queued`로 전환한다.
- `SuspendedDbCommand`이면 completion을 기록하고 `Suspended -> Queued`로 전환한다.
- 불일치 completion은 state를 변경하지 않고 stale metric만 올린다.

어느 경우에도 completion 함수가 coroutine을 inline resume하지 않는다.

Timeout 역시 `AwaitKey`와 현재 blocked task가 일치할 때만 유효하다. physical cancel이 불가능해도
Actor는 timeout completion으로 진행하며 늦은 DB completion은 stale로 폐기한다. mutation query 자동
retry는 금지하고 idempotent operation 또는 idempotency key가 있을 때만 상위 정책에서 수행한다.

Native DB driver의 acquire, DNS, connect, TLS/auth, submit, partial read/write, result fetch, cancel과
reconnect 전체 경로가 non-blocking이어야 한다. hidden synchronous fallback은 허용하지 않는다.

8단계 conformance 게이트가 MySQL 8.4와 libmysqlclient 21.2에서 실측한 결과는 다음과 같다.

| 항목 | 실측 |
| --- | --- |
| async API | `mysql_*_nonblocking`. socket은 `MYSQL::net.fd` |
| 대기 방향 힌트 | **없다.** `net.reading_or_writing`은 모든 `NOT_READY`에서 0이다 |
| TLS handshake | connect 한 호출이 스레드를 약 8ms 점유한다. `ssl_mode=DISABLED`면 약 100us |
| DNS | connect 안에서 동기 호출된다 |
| streaming | `mysql_use_result()` + `mysql_fetch_row_nonblocking()`으로 가능하다 |
| cancel | 별도 control connection의 server-side KILL만 가능하다 |

여기서 나온 계약은 다음과 같다.

- `ssl_mode`는 설정에서 명시한다. client 기본값이 `PREFERRED`라 명시하지 않으면 connect마다 TLS 비용을
  조용히 지불한다.
- host는 IP만 받는다. 호스트명 해석은 startup 1회로 loop 밖에서 한다.
- poll interest는 phase에서 나온다. server greeting을 받은 뒤의 handshake 구간에만 writability를 함께
  걸고, 나머지 phase는 read-only다. greeting 이전에 writability를 걸면 응답 없는 서버에서 Worker가
  spin한다.
- SELECT는 streaming으로 받고 row/byte 상한을 fetch 도중에 강제한다. 조기 중단은 `Commands out of sync`를
  피하기 위해 connection을 폐기한다.
- DB event 하나의 progress는 step/row/byte/duration으로 bound한다.
- timeout은 queued와 in-flight를 구분한다. queued는 connection을 건드리지 않는다.
- `mysql_library_init/end`의 owner는 WorkerGroup이 아니라 application이며, `MYSQL*` 생성은 owner
  Worker thread에서 한다.

async는 처리 용량을 무한으로 만들지 않는다. connection, in-flight, queued count와 request/result byte를
함께 제한한다.

| 항목 | 초기 기준 |
| --- | ---: |
| Worker당 DB connection | 2 |
| connection당 in-flight | 기본 1, driver가 multiplexing을 보장할 때만 증가 |
| DB-local queued request | Worker당 256 operations + byte cap |
| Actor당 awaited DB operation | 1 |
| 기본 query timeout | 2 s |
| operation당 buffered result | 4 MiB |

## 11. Bounded capacity, ordering과 stale safety

초기 용량은 배포 메모리 예산과 부하 테스트로 조정한다. 상한의 존재 자체는 필수다.

| 대상 | 초기 기준 | overflow |
| --- | ---: | --- |
| WorkerInbox data | 65,536 events / 64 MiB | `tryPush` 실패 |
| Actor mailbox | 1,024 messages / 4 MiB | message별 reject/coalesce/drop |
| ActorTable | 배포 설정 hard cap | 신규 activation overload |
| Concurrent Loading | DB와 메모리 기준 hard cap | 신규 activation loading limit |
| TimerQueue | Worker별 hard cap | schedule 실패 |
| DB-local queue | 256 operations + byte cap | 즉시 overload |
| Blocking adapter queue | backend별 bounded | 즉시 overload |
| Connection write | soft 1 MiB / hard 4 MiB | reject/coalesce 후 hard close |

권장 mailbox overflow 정책은 Player command reject, position/tick coalesce, telemetry drop, payment/reward
reject + durable retry/idempotency다. 중요한 command를 조용히 drop하지 않는다.

Ordering 보장 범위는 다음과 같다. 표에 없는 조합은 보장하지 않는다. 더 강한 순서가 필요한 프로토콜은
sequence number나 expected version을 message payload에 넣는다.

| 범위 | 보장 | 비보장 |
| --- | --- | --- |
| 한 Actor mailbox | dequeue 순서대로 serial 처리 | 여러 source Worker 사이의 global order |
| 같은 source Worker -> 같은 target Worker | 동일 inbox lane의 enqueue 순서 | 다른 source와의 상대 순서 |
| `EffectBatch` | batch 안의 ordered list 순서대로 apply | 다른 Actor turn과의 atomic grouping |
| Connection write buffer | owner Worker의 append 순서 | 서로 다른 connection 사이의 순서 |
| Awaited command | 완료 전에는 다음 mailbox command를 실행하지 않음 | 다른 Actor와의 상대 완료 순서 |
| Timer | deadline 이전 실행 금지 | 같은 deadline 사이의 total order |

Application timer가 만료되면 TimerQueue entry를 먼저 제거하여 entry/byte accounting을 정확히 한 번
반환한다. 그 뒤 ActorKey + incarnation을 검증하고 mailbox admission을 별도로 수행한다. 성공하면 mailbox가
message와 자신의 count/byte charge를 소유하고, stale activation·Stopping·mailbox full이면 message를
파괴한다. TimerQueue와 mailbox accounting은 같은 logical charge를 사용해도 서로 독립된 resource다.

Stale 검증 key는 다음과 같다.

| 이벤트 | 검증 key |
| --- | --- |
| DB/adapter completion | ActorKey + Incarnation + OperationId |
| activation-bound timer | ActorKey + Incarnation |
| await timeout | AwaitKey + current `ActorSlot.blocked` |
| connection send/close | ConnectionId + Generation |
| DB poll event | DbConnectionId + Generation |

다른 Worker queue가 비기를 기다리거나 spin하지 않는다.

## 12. Workflow 적용 우선순위

Actor-to-Actor 응답을 일반 coroutine await로 만들지 않는다. Suspended Actor는 mailbox command를 실행하지
않기 때문에 mailbox 응답을 기다리면 deadlock성 설계가 된다.

```text
1. 한 Actor command가 DB/native I/O만 기다림
   -> 해당 Actor coroutine + concrete suspended DB task

2. 여러 Actor message가 오가고 자연스러운 domain owner가 있음
   -> 그 Actor의 explicit workflow state + correlation ID
   -> 요청 turn을 끝내고 응답을 새 mailbox message로 처리

3. 독립 ID/lifecycle로 여러 Actor 응답을 조정
   -> Coordinator Actor의 explicit message-driven state

4. process restart 후 복구 필요
   -> durable workflow system
```

Coordinator Actor는 특정 domain Actor에 귀속되지 않고, 여러 요청자가 조회/취소하거나 initiating
Actor보다 오래 살며 독립 timeout/state query가 필요할 때만 만든다.

## 13. Startup과 shutdown

Startup:

```text
1. 설정과 모든 capacity 검증
2. Worker 생성
3. Worker-local Poller와 선택 DbClient 초기화
4. Worker thread 시작
5. Listener 시작과 connection 배정
```

Graceful shutdown:

```text
1. Listener stop
2. stop_requested 설정 + Worker wakeup
3. 신규 외부 command 제한
4. 신규 일반 DB/adapter submit 제한
5. 기존 inbox, Actor turn과 native DB completion 처리
6. 각 ActorSlot.blocked logical cancel
7. connection write drain
8. hard deadline 뒤 남은 coroutine/resource 정리
9. Worker thread join
```

`QuiescingActors`에 들어가면 신규 application ingress를 막고 application timer entry를 실제로 제거·파괴한
뒤 accounting을 반환한다. 그 다음 blocked Actor cancel과 runnable work drain을 수행한다. live application
timer 또는 live `TimerReservation` 중 하나라도 0이 아니면 Worker는 Actor quiescence를 선언하지 않는다. Await
timeout과 connection close deadline은 application timer cancellation 대상이 아니다.

WorkerInbox와 poller는 completion producer가 종료될 때까지 살아 있어야 한다. Worker 하나의 invariant
위반은 기본적으로 process fail-fast다. 부분 Worker 재시작은 v1.3에서 지원하지 않는다.

6단계에서는 cross-worker Actor가 처음 생기므로 10단계의 shutdown 작업 중 최소 loss-prevention만
앞당긴다. WorkerGroup stop은 32-bit participant mask, 30-bit publication epoch, armed/aborted flag를
단일 64-bit atomic에 저장한다. 성공한 remote enqueue는 target bit를 clear하고 target이 이미 active여도
epoch를 증가시킨다. Worker는 empty scan 전에 읽은 epoch가 그대로일 때만 quiescence를 commit한다.
shutdown 한 번에 30-bit epoch가 소진되면 wrap하지 않고 abort하여 forced cleanup으로 전환한다.
watchdog, 장시간 shutdown 부하와 test-path 품질 검증은 10단계에서 완료했다. 실제 MySQL과 production
application workflow를 포함한 동일 gate 재실행은 11단계에 남긴다.

## 14. 구현 전환 순서

| 단계 | 산출물 | 완료 기준 |
| --- | --- | --- |
| 1 | ActorKey, ActivationRef, ConnectionRef, AwaitKey | stale validation unit test |
| 2 | Poller, WorkerInbox, TimerQueue, budget loop | empty loop와 shutdown 안정 |
| 3 | ConnectionTable, decode, direct write buffer | protocol과 slow-consumer test |
| 4 | ActorTable, mailbox, ReadyActorQueue, bounded activation | single-worker deterministic test |
| 5 | ActivationLoad와 SuspendedDbCommand | Loading/Suspended completion test |
| 6 | concrete WorkerEvent와 local/remote tell/send | source별 SPSC stress, reject path와 최소 group quiescence |
| 7 | `toEffects` overload와 ordered EffectBatch | effect order와 failure test |
| 8 | Worker-local DbClient와 `completeDb` | driver conformance와 stale test |
| 9 | 선택 blocking/CPU adapter | saturation과 cancel test |
| 10 | metrics, watchdog, shutdown과 load test | test path 품질 게이트 통과(TCP 통과, MySQL 미측정 명시) — 완료 |
| 11 | legacy ActorRuntime/Binding/Outbound hop 제거 | 신규 경로 100% 전환 |

기존 책임은 다음처럼 이동한다.

| 기존 구조 | 목표 구조 |
| --- | --- |
| ActorRuntime registry/scheduler | Worker의 ActorTable + ReadyActorQueue |
| ActorBinding | typed dispatch + `toEffects` 함수 |
| CrossWorkerInbox + CompletionQueue | 논리적 WorkerInbox 하나; native DB completion은 same-thread direct call |
| OutboundSink/OutboundQueue | owner Worker의 ConnectionSlot write buffer |
| PendingOperationTable | 제거; Actor state는 `ActorSlot.blocked`, protocol state는 `DbClient::in_flight` |
| AsyncGateway | 제거; `DbClient::tryStart()` |
| CompletionDispatcher | 제거; 구체 completion 함수 |
| WorkflowTable | explicit Actor workflow state 또는 필요 시 Coordinator Actor |
| BlockingExecutor | 선택 `BlockingAdapterExecutor` |

## 15. 승인 기준

다음 질문에 한 문장으로 답할 수 있어야 한다.

1. mutable state의 owner는 누구인가? — 하나의 Worker다.
2. owner가 다르면 어떻게 접근하는가? — WorkerInbox에 concrete WorkerEvent를 보낸다.
3. Actor는 언제 실행되는가? — ReadyActorQueue의 turn에서 command 또는 continuation 하나를 실행한다.
4. Actor가 무엇을 기다리는지는 어디에 있는가? — `ActorSlot.blocked` 한 곳에 있다.
5. DB backend는 무엇을 소유하는가? — connection과 wire/protocol 진행 상태만 소유한다.
6. async 완료 후 어디서 이어지는가? — 시작한 owner Worker의 Actor phase에서 이어진다.
7. overload 시 어떻게 되는가? — 기다리지 않고 bounded admission에서 실패한다.

품질 게이트는 cross-thread mutable access 0건, Worker blocking 0건, 모든 resource의 memory bound,
single blocked-state source, stale mutation 0건, phase starvation 없음과 hard deadline 안의 leak 없는
shutdown이다.
