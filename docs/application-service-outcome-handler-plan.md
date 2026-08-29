# Application Service + OutcomeHandler 구조 통합 계획

> 상태: 구현 전 확정안  
> 범위: ProtocolGateway, Player/Zone/Room application 경계, actor/workflow outcome 실행, typed tell/timer, 기존 protocol sink/mapper 제거  
> 기준: 현재 작업 트리의 변경을 보존하며 wire format, Actor FIFO, admission 정책, lifecycle 및 metric 의미를 바꾸지 않는다.

## 1. 목표와 비목표

입력은 Application Service에서, 출력은 OutcomeHandler에서 합류시킨다.

```text
Frame
  ↓
ProtocolGateway
  ↓ typed request
Application Service ───────────────→ Workflow
  │                                    │
  │                                    └─ typed workflow emission
  ↓
ActorRuntime → ActorBinding → Domain Result
                              │
post-accept runtime outcome ──┤
                              ▼
                       OutcomeHandler
                 runtime / transition / client effects
                              │
                           encode_*()
                              │
                            Frame
                              │
                         OutboundSink
```

용어와 책임을 다음처럼 고정한다.

```text
Gateway        : bytes/frame → typed request 또는 protocol-level rejection
Service        : request → application validation/orchestration/admission
Actor/Workflow : accepted operation → typed outcome
OutcomeHandler : outcome + effect context → ordered effects
Encoder        : typed response → Frame
OutboundSink   : bounded transport resource execution
```

비목표:

- wire format, `MessageType`, request/response 의미를 변경하지 않는다.
- Actor key, worker sharding, FIFO, turn budget 또는 admission capacity를 변경하지 않는다.
- Domain Result를 이 작업에서 다른 namespace로 옮기지 않는다. 현재 `snf::server` namespace 정리는 별도 작업으로 남긴다.
- Workflow의 보상 규칙이나 route/session 소유권 자체를 재설계하지 않는다.
- unified worker 또는 별도 timer service를 도입하지 않는다.
- 공통 virtual `OutcomeHandler` 기반 클래스를 만들지 않는다.

## 2. 경계 불변식

금지사항:

- Gateway는 session/route/membership을 조회하거나 workflow를 선택하지 않는다.
- Service와 Workflow는 Domain mutable state를 직접 변경하지 않는다.
- Binding은 생성된 Domain Result의 필드를 분기해 effect를 수행하지 않는다.
- OutcomeHandler는 Domain을 재조회·재실행하거나 gameplay state를 변경하지 않는다.
- Encoder는 routing, capacity, retry, connection close를 모른다.
- OutboundSink는 메시지 의미를 모른다.
- Workflow는 OutcomeHandler, Encoder, OutboundSink를 참조하지 않는다.
- Handler와 기존 sink가 같은 outcome을 이중 실행하는 과도기 wiring을 허용하지 않는다.

client-visible outcome의 범위는 다음처럼 제한한다.

> Application/Domain에서 요청이 수락된 뒤 발생한 모든 client-visible outcome은 OutcomeHandler를 거친다. Frame framing/decoding 실패, unknown `MessageType`, invalid wire encoding 및 요청 수락 전의 top-level admission 실패는 Gateway/TcpServer의 protocol/admission policy를 따른다.

구체적으로:

| 시점 | 예 | 종료 경로 |
| --- | --- | --- |
| frame 해석 전/중 | malformed frame, unknown type, 잘못된 길이·endian·enum | `InvalidPayload`/`UnsupportedMessage` → TcpServer protocol policy |
| application 수락 전 | session/route 불일치, 첫 actor ingress가 `Full`/`Closed` | `FramePostResult` → TcpServer admission policy |
| application 수락 후 | workflow 다음 단계 post 실패, compensation/return admission 실패 | typed workflow/runtime outcome → OutcomeHandler |
| actor 내부 실행 중 | Room deadline 선예약 실패, outbound 예약 실패 | typed runtime outcome → OutcomeHandler |

이 구분으로 top-level `FramePostResult`와 post-accept `*AdmissionFailure`를 같은 개념으로 섞지 않는다.

## 3. Outcome 계약

### 3.1 Compile-time shape

공통 virtual 기반 클래스 대신 C++20 concept를 사용한다.

```cpp
template<class Handler, class Context, class Outcome, class Return>
concept HandlesOutcome =
    requires(Handler& handler, Context context, Outcome outcome)
    {
        {
            handler.handle(
                std::move(context),
                std::move(outcome))
        } -> std::same_as<Return>;
    };
```

Concrete Handler:

```text
PlayerOutcomeHandler
ZoneOutcomeHandler
RoomOutcomeHandler
```

각 public overload는 가까운 header/test translation unit에서 `static_assert`한다.

```cpp
static_assert(HandlesOutcome<
    PlayerOutcomeHandler,
    PlayerEffectContext,
    PlayerResult,
    ActorTask<ActorDispatchResult>>);

static_assert(HandlesOutcome<
    ZoneOutcomeHandler,
    ZoneEffectContext,
    ZoneResult,
    ActorDispatchResult>);

static_assert(HandlesOutcome<
    RoomOutcomeHandler,
    RoomEffectContext,
    RoomResult,
    ActorDispatchResult>);
```

Workflow/runtime outcome overload도 각각 검증한다. 하나의 거대한 `Outcome` variant나 추상 `DeliveryFailure`를 공통 계약으로 만들지 않는다. Domain별 작은 variant는 exhaustive visitation이 유용한 경우에만 허용한다.

### 3.2 Outcome 종류

```text
Domain Result
Workflow Outcome
post-accept Runtime/Admission Outcome
```

가짜 command 또는 가짜 Domain Result로 실패를 표현하지 않는다.

예:

```cpp
struct RoomRuntimeOverloaded;
struct ZoneClientReply;
struct RoomJoinedReply;
struct ReturnedToZoneReply;
struct ZoneAdmissionFailure;
struct RoomAdmissionFailure;
```

`ClientEffectContext`가 connection/request identity를 소유하고 outcome에는 중복 저장하지 않는다.

```cpp
struct ClientEffectContext
{
    ConnectionId connection;
    std::uint32_t request_id;
};
```

Workflow가 비동기 완료를 내보낼 때는 context와 outcome을 한 emission으로 묶되, 같은 ID를 두 위치에 복제하지 않는다.

```cpp
struct ZoneWorkflowEmission
{
    ClientEffectContext context;
    ZoneWorkflowOutcome outcome;
    CommandReleaseToken release;
};
```

client context가 필요 없는 emission은 별도 context를 사용한다. `std::optional<ClientEffectContext>`를 모든 outcome에 무차별적으로 넣지 않는다.

`CommandReleaseToken`은 Handler 입력이 아니다. Workflow가 emission으로 소유권을 넘기고 Service가 forwarding 호출 동안 지역 RAII guard로 보유한다. Handler가 정상 반환하거나 예외가 전파돼 stack unwind가 일어난 직후 guard의 destructor가 token을 정확히 한 번 release한다. 현재 token의 private `release()`를 public API로 바꾸거나 Handler가 command lifecycle을 알게 만들지 않는다.

### 3.3 OutcomeHandler thread-safety와 lifetime

`GameServer`가 소유하는 domain별 Handler는 reactor/application thread와 하나 이상의 Actor Worker에서 공유될 수 있다.

불변식:

> OutcomeHandler는 호출 간 mutable execution state를 보관하지 않는다. 설정과 dependency reference는 immutable이며, 공유 통계는 atomic/thread-safe primitive로 관리한다. suspend에 필요한 상태는 호출별 coroutine frame에만 존재한다.

- outbound reservation, snapshot transaction, retry progress, 임시 Frame은 Handler 멤버에 저장하지 않는다.
- `ActorTask` handle은 resume/lifetime을 위해 해당 ActorState가 소유할 수 있지만, 대기 중 값과 진행 단계는 coroutine frame이 소유한다.
- 서로 다른 Player actor가 같은 `PlayerOutcomeHandler`에서 동시에 suspend돼도 상태가 섞이지 않아야 한다.
- Handler가 참조하는 `OutboundSink`, `PlayerSessionDirectory`, transition channel 등의 동시성 계약을 header에 명시한다. thread-safe하지 않은 dependency는 thread-safe channel을 통해서만 호출한다.
- Handler coroutine이 보유한 `ActorContext`/capability reference는 Actor가 suspended 상태로 유지되는 동안 유효해야 하며, task 파괴가 Actor state 파괴보다 먼저 일어나야 한다.
- 공유 통계 snapshot은 TSan에서 data race 없이 읽을 수 있어야 한다.

잘못된 형태:

```cpp
class PlayerOutcomeHandler
{
    std::optional<OutboundReservation> _pending_reservation; // 금지
};
```

의도한 형태:

```cpp
ActorTask<ActorDispatchResult>
PlayerOutcomeHandler::handle(PlayerEffectContext context, PlayerResult result)
{
    auto reservation = co_await reserveResponses(context, result);
    // reservation과 suspend 진행 상태는 이 호출의 coroutine frame에만 존재한다.
}
```

### 3.4 Effect context와 제한된 capability

Outcome과 effect 실행에 필요한 입력 메타데이터를 분리한다.

- reply kind, connection/request ID, transition ticket는 effect context에 둔다.
- `ZoneResult`/`RoomResult`는 Domain fact만 담는다.
- Handler에 전체 Actor나 mutable Domain 객체를 전달하지 않는다.
- ActorContext는 tell/timer/async operation 같은 runtime capability만 제공한다.
- Player persistence에는 snapshot admission과 rollback만 가능한 제한된 capability를 제공한다.

```cpp
class PlayerSnapshotAccess
{
public:
    [[nodiscard]] std::optional<PlayerSnapshotTransaction> tryTake();
    void restore(PlayerStateComponentMask) noexcept;
};
```

`PlayerSnapshotAccess`는 gameplay command, balance, inventory 또는 임의 state mutation API를 노출하지 않는다.

## 4. Domain Result fact와 유효성

Domain은 runtime disposition을 반환하지 않고 처리 직후의 lifecycle fact를 반환한다.

```cpp
struct ZoneResult
{
    // 기존 결과
    bool empty{true};
};

struct RoomResult
{
    // 기존 결과
    RoomPhase phase{RoomPhase::Waiting};
    std::size_t participant_count{0};
};
```

모든 early return을 포함해 `empty`와 `participant_count`는 command 처리 후 state를 반영한다. Binding/Handler가 `Zone::playerCount()`나 `Room::participantCount()`를 다시 조회하지 않는다.

### 4.1 금지 조합

Zone:

```text
empty == true
→ tick_after == nullopt
```

Room:

```text
phase in {Cleared, Failed} OR participant_count == 0
→ tick_after == nullopt

outcome.has_value()
→ phase in {Cleared, Failed}
  AND tick_after == nullopt
  AND deadline_after == nullopt

failure_reason.has_value()
→ outcome == BattleOutcome::Failed

grants not empty
→ outcome == BattleOutcome::Cleared

deadline_after.has_value()
→ successful StartBattle transition to Running
  AND tick_after.has_value()
```

`deadline_after`는 “현재 deadline이 존재한다”는 지속 상태가 아니라 성공한 `StartBattle`이 한 번 내보내는 schedule intent다.

Handler는 모순된 Result의 우선순위를 정하지 않는다. migration 동안 `Zone::makeResult`와 `Room::baseResult` 같은 단일 생성 경로에서 fact를 채우고 debug validation을 수행하며, Domain 단위 테스트로 모든 반환 경로를 고정한다. 위반은 resource failure가 아니라 programming error다.

### 4.2 Disposition 변환

```text
ZoneResult.empty
  true  → PassivateIfIdle
  false → KeepActive

RoomResult.phase terminal OR participant_count == 0
  true  → PassivateIfIdle
  false → KeepActive
```

`passivate_if_idle` 같은 runtime hint는 Result에 추가하지 않는다.

## 5. Typed tell과 self timer

### 5.1 실제 C++ API shape

`ActorContext`는 virtual interface이므로 virtual template을 만들지 않는다. public typed facade와 private type-erased virtual bridge를 사용한다.

```cpp
class ActorContext
{
public:
    template<class Message>
    [[nodiscard]] PostResult tryTell(ActorKey target, Message message)
    {
        return tryTellPayload(
            target,
            TellPayload::of(std::move(message)));
    }

    template<class Message>
    [[nodiscard]] std::optional<TimerHandle> tryScheduleSelf(
        std::chrono::milliseconds delay,
        Message message)
    {
        return tryScheduleSelfPayload(
            delay,
            TellPayload::of(std::move(message)));
    }

private:
    virtual PostResult
    tryTellPayload(ActorKey, TellPayload) = 0;

    virtual std::optional<TimerHandle>
    tryScheduleSelfPayload(
        std::chrono::milliseconds,
        TellPayload) = 0;
};
```

`ActorRuntime`의 외부 typed tell도 같은 facade를 사용한다. `TellPayload`와 raw virtual bridge는 runtime 구현 경계에만 남기고 Handler는 직접 만들지 않는다.

지원 메시지:

- Zone: `ZoneSimulationTick`
- Room: `RoomJoinTell`, `RoomSimulationTick`, `BattleDeadline`
- Player: `StreetExperienceGrant`, snapshot retry message

각 Binding의 `makeTell()`은 해당 kind가 지원하는 typed message를 `ActorSubmission`으로 변환한다. target/payload identity 불일치와 미지원 타입은 `std::nullopt`이며 Runtime이 programming error로 승격한다.

### 5.2 공통 translation 경로

```text
즉시 tell
ActorContext/ActorRuntime::tryTell<T>()
  → tryTellPayload()
  → ActorRuntime::translateTell()
  → ActorSubmission
  → tryPost()가 outstanding slot 예약

지연 self tell
ActorContext::tryScheduleSelf<T>()
  → tryScheduleSelfPayload()
  → outstanding slot 선예약
  → TimerEntry{self key, incarnation, handle, TellPayload}

timer 발화
  → ActorRuntime::translateTell()
  → ActorSubmission
  → 이미 예약된 slot의 소유권을 mailbox로 이전
```

Timer 발화 시 public `tryTell()`/`tryPost()`를 호출하지 않는다. timer는 schedule 시 outstanding slot을 이미 예약했기 때문이다. payload 변환과 Binding 선택만 즉시 tell과 동일한 `translateTell()`을 사용한다.

### 5.3 Timer slot accounting

하나의 예약 slot은 다음 상태 중 정확히 하나가 소유한다.

```text
schedule 성공
  timer owns reserved slot
    ├─ cancel/passivation purge/shutdown/stale/translation failure
    │    → release exactly once
    └─ fire + mailbox enqueue
         → mailbox/active command owns same slot
              → command terminal 시 release exactly once
```

추가 불변식:

- schedule capacity 부족은 `std::nullopt`이고 TimerEntry가 생기지 않는다.
- TimerEntry 삽입 또는 carrier 이동이 예외를 던지면 선예약 slot을 rollback한다.
- timer handle은 owning Worker와 self actor 범위에서만 취소할 수 있다.
- fire 시 actor 부재 또는 incarnation 불일치는 stale discard이며 slot을 반환한다. actor를 재활성화하지 않는다.
- `PassivateIfIdle`/`Evict`가 actor를 제거할 때 해당 actor의 timer를 purge하고 각 slot을 한 번만 반환한다.
- fire 시 `translateTell()` 거부는 programming error다. reserved slot을 반환한 뒤 worker failure policy를 따른다.

## 6. OutcomeHandler effect 계약

### 6.1 공통 실행 규칙

- Handler는 전달받은 outcome을 정확히 한 번 해석한다.
- 순서는 아래에 적힌 effect의 시도 순서다. client send 실패가 이미 필요한 cleanup/transition effect를 조용히 생략하게 만들지 않는다.
- 예상 가능한 capacity/resource 거부는 typed outcome 또는 기존 failure policy로 처리한다.
- invalid Result, unsupported typed tell, impossible lifecycle state는 `logic_error` 계열 programming error다.
- shutdown cancellation은 `Stopped`, 정상 resource 포화는 기존 domain별 fail-fast/suspend 정책으로 구분한다.
- command lifecycle이 있는 Workflow emission은 `CommandReleaseToken` 소유권을 Service로 넘긴다. Service가 terminal effect forwarding scope 동안 RAII로 보유하고 정확히 한 번 release하며 Handler는 token을 알지 못한다.

### 6.2 Player

순서:

1. room join typed tell
2. undelivered entry completion
3. dirty snapshot enqueue/rollback/retry 예약
4. response batch slot 수 계산
5. 즉시 예약 또는 이 호출의 coroutine frame에서 suspend/resume
6. pure encoder 호출과 reserved batch commit
7. disposition 반환

`PlayerActorBinding`은 Domain 실행 전 load와 connection-close final save를 계속 소유한다. post-domain effect coroutine만 `PlayerOutcomeHandler`로 이동한다.

Binding의 ActorState는 Handler task handle을 보관할 수 있지만 `pending_result`, outbound reservation, Handler 전용 stage를 별도로 복제하지 않는다. snapshot retry message도 typed self timer를 사용한다.

### 6.3 Zone

순서:

1. transition completion 또는 client reply
2. `tick_after`가 있으면 typed self timer 예약
3. `empty` fact로 disposition 결정

`empty && tick_after`는 우선순위로 해결하지 않고 invalid Result로 거부한다.

### 6.4 Room

순서:

1. Binding이 Domain 실행 전에 선예약한 deadline timer 유지/취소
2. `tick_after`가 있으면 typed self timer 예약
3. transition completion 또는 direct reply
4. digest/clear/failure fanout
5. return request publish
6. grant typed tell
7. `phase`와 `participant_count`로 disposition 결정

`StartBattle` deadline 선예약 실패 시 Binding은 Domain을 호출하지 않고 `RoomRuntimeOverloaded`와 pre-domain effect context를 Handler에 전달한다. Handler는 fake `RoomResult`를 만들지 않는다.

terminal/empty Room은 tick을 예약하지 않는다. 성공한 deadline 선예약은 actor passivation/eviction 시 Runtime timer purge 대상이며, Result가 start를 확정하지 않으면 같은 호출에서 즉시 취소한다.

## 7. Encoder와 delivery

다음 pure free function을 추출한다.

```text
encode_player_response()
encode_zone_reply()
encode_room_command_reply()
encode_battle_digest()
encode_battle_cleared()
encode_battle_failed()
encode_room_joined()
encode_returned_to_zone()
```

전송 정책:

- Player: 전체 frame 수를 먼저 계산하고 batch 예약한다. 즉시 포화면 해당 Actor만 suspend한다.
- Zone: 연결별 단일 Frame fail-fast.
- Room: audience를 Domain Result에서 확정한 뒤 연결별 단일 Frame fail-fast.
- 단일 Frame 전송 실패는 `OutboundSink::reportAdmissionFailure(connection)`으로 연결 종료를 요청한다.
- encoder는 allocation/size validation 외에 전송이나 route lookup을 하지 않는다.

Binding, Service, Workflow는 `OutboundSink`와 encoder를 직접 참조하지 않는다.

## 8. Workflow와 Service의 비동기 경계

기존 객체를 다음처럼 역할별로 분리한다.

```text
ZoneHandoffService → ZoneHandoffWorkflow + ZoneService
RoomEntryService   → RoomEntryWorkflow   + RoomService
```

Workflow는 Handler를 멤버로 보관하거나 직접 호출하지 않는다. 비동기 workflow의 “outcome 반환”은 다음 두 경로로 고정한다. 잘못된 `admission`/`terminal` 조합을 aggregate로 표현하지 않도록 시작 결과 자체를 상태 variant로 만든다.

```cpp
enum class WorkflowRejection
{
    InvalidPayload,
    Full,
    Closed,
};

struct WorkflowRejected
{
    // Accepted를 표현할 수 없는 domain-specific rejection이다.
    WorkflowRejection reason;
};

struct WorkflowCompleted
{
    WorkflowEmission terminal;
};

struct WorkflowPending
{
};

using WorkflowStartResult = std::variant<
    WorkflowRejected,
    WorkflowCompleted,
    WorkflowPending>;

WorkflowStartResult ZoneHandoffWorkflow::tryStart(...);
std::vector<ZoneWorkflowEmission> ZoneHandoffWorkflow::drain();
```

- `WorkflowRejected`: `WorkflowRejection`을 `FramePostResult`로 변환한다. `Accepted`를 표현할 수 없고 emission/token/active workflow state가 없다.
- `WorkflowCompleted`: admission은 암묵적으로 `Accepted`이고 terminal emission을 즉시 한 번 반환한다. active workflow state를 남기지 않으며 같은 operation이 `drain()`에서 다시 나오지 않는다.
- `WorkflowPending`: admission은 암묵적으로 `Accepted`이고 정확히 하나의 active workflow state가 존재한다. 정상 실행에서는 terminal emission이 이후 `drain()`에서 정확히 한 번 나온다.
- shutdown `cancel()`은 Pending의 유일한 별도 terminal 경로다. client emission 대신 active state 정리, transport shutdown 및 token RAII release를 수행하며 동일 operation이 이후 `drain()`에 나타나지 않는다.
- 이후 transition completion은 bounded `drain()`이 typed emission 목록으로 반환한다.
- emission 수는 기존 `max_*_completions_per_turn`으로 제한하고 vector capacity도 그 상한에 맞춘다.
- Workflow는 route/session 변경, actor stage post, rollback/compensation state machine과 workflow stats를 계속 소유한다.
- Workflow emission은 Handler가 effect를 실행하는 데 필요한 완결된 fact를 소유하며 Workflow 내부 map의 reference를 노출하지 않는다.

Service가 유일한 forwarding 지점이다.

```cpp
void ZoneService::forward(ZoneWorkflowEmission emission)
{
    [[maybe_unused]] auto terminal_guard =
        std::move(emission.release);

    _outcomes.handle(
        std::move(emission.context),
        std::move(emission.outcome));
    // 정상 반환과 예외 unwind 모두 이 scope 끝에서 exactly-once release.
}

void ZoneService::drain()
{
    auto emissions = _handoff_workflow.drain();
    for (auto& emission : emissions)
    {
        forward(std::move(emission));
    }
}
```

`tryStart()`의 `WorkflowCompleted`도 같은 `forward()`를 사용한다. `WorkflowRejected`는 저장된 state 없이 `reason`을 대응하는 `FramePostResult`로 변환해 반환하고, `WorkflowPending`은 `Accepted`를 반환한다. 실제 outcome/domain별 context 타입은 달라도 이 소유 방향과 token release 지점을 혼용하지 않는다.

### 8.1 Service 책임

`PlayerService`:

- authentication/session attach와 rollback
- provisional/persistent Player 선택
- Player command admission
- connection-close coordination
- close 시 Zone/Room cleanup과 Player final close 순서 유지
- `ZoneConnectionCleanup`/`RoomConnectionCleanup`의 좁은 port를 통해서만 다른 domain cleanup을 요청

`ZoneService`:

- enter/move/leave session/route 검증
- direct command와 handoff workflow 선택
- Zone Actor admission
- busy/immediate workflow outcome forwarding
- handoff drain/close/cancel/stats 위임

`RoomService`:

- membership/room identity 검증
- battle command admission
- room entry/return workflow 선택
- busy/immediate workflow outcome forwarding
- entry/return drain/close/cancel/stats 위임

Service는 direct actor ingress를 사용한다. `RoutedCommandIngress`/`CommandRouter`는 모든 Service와 Workflow가 typed ingress로 이전한 뒤 제거한다.

connection-wide close orchestration은 `PlayerService`가 소유하는 것으로 고정한다. `PlayerService`가 Zone/Room Service 전체 API에 의존하지 않도록 두 Service가 각각 좁은 cleanup port를 구현한다.

```cpp
class ZoneConnectionCleanup
{
public:
    virtual ~ZoneConnectionCleanup() = default;
    [[nodiscard]] virtual PostResult
    tryCleanup(const ConnectionClosed&) = 0;
};

class RoomConnectionCleanup
{
public:
    virtual ~RoomConnectionCleanup() = default;
    [[nodiscard]] virtual PostResult
    tryCleanup(const ConnectionClosed&) = 0;
};
```

`PlayerService::tryPostConnectionClosed()`는 pending workflow disconnect 표시, Zone cleanup, Room cleanup, Player final close 순서를 기존 동작과 같게 유지한다. `Full`이면 즉시 반환되어 TcpServer가 재시도할 수 있으므로 각 cleanup port는 같은 `ConnectionClosed`의 재호출에 idempotent해야 한다. 이미 완료한 route leave/abandon을 중복 publish하거나 lifecycle token을 중복 release하지 않는다.

### 8.2 Composition root

```text
GameServer
├─ PlayerOutcomeHandler
├─ ZoneOutcomeHandler
├─ RoomOutcomeHandler
├─ ZoneHandoffWorkflow
├─ RoomEntryWorkflow
├─ ZoneService
│    ├─ ZoneActorIngress
│    ├─ ZoneHandoffWorkflow&
│    ├─ ZoneOutcomeHandler&
│    └─ implements ZoneConnectionCleanup
├─ RoomService
│    ├─ Player/Zone/Room typed ingress
│    ├─ RoomEntryWorkflow&
│    ├─ Room/Zone OutcomeHandler&
│    └─ implements RoomConnectionCleanup
├─ PlayerService
│    ├─ PlayerActorIngress
│    ├─ ZoneConnectionCleanup&
│    └─ RoomConnectionCleanup&
└─ ProtocolGateway
     ├─ PlayerService&
     ├─ ZoneService&
     └─ RoomService&

ZoneActorBinding ─→ ZoneOutcomeHandler&
RoomActorBinding ─→ RoomOutcomeHandler&
PlayerActorBinding → PlayerOutcomeHandler&
```

Workflow의 actor completion은 Binding이 Handler를 통해 transition channel에 publish하고, reactor가 Service의 `drain()`을 호출해 Workflow outcome을 다시 Handler로 전달한다.

## 9. ProtocolGateway

Gateway가 담당:

- `MessageType` dispatch
- payload 길이/endian/enum 검증
- typed request 생성
- 해당 Service 호출
- Service의 `FramePostResult`를 그대로 protocol ingress에 반환

Gateway에서 제거:

- session/route/membership 조회
- Actor key 결정
- direct/workflow 선택
- attach/route rollback
- `RoutedCommandIngress`
- handoff/room-entry 내부 객체 참조
- application reply 생성

`MessageDispatcher`는 각 메시지별 `decode_*()` pure/free function의 byte parity test를 먼저 만든 뒤 제거한다.

connection close와 lifecycle forwarding은 `FrameIngress` 계약 때문에 Gateway에 entry point가 남을 수 있지만, 실제 정리 순서와 route/session 정책은 `PlayerService` 및 domain Service가 소유한다.

## 10. Metric 소유권과 호환성

동작과 함께 metric update 지점도 이동한다.

| 현재 metric | 새 내부 소유자 | 외부 의미 |
| --- | --- | --- |
| Zone/Room command·tick 실행 시간, tick overrun | Binding | 유지 |
| Room tick/deadline schedule rejection, grant tell rejection | RoomOutcomeHandler | 유지 |
| Room publish/fanout 통계 | RoomOutcomeHandler | 유지 |
| Player snapshot admission/retry give-up | PlayerOutcomeHandler | 유지 |
| Player load failure | PlayerActorBinding | 유지 |
| workflow transition/compensation/stale completion | 각 Workflow | 유지 |
| actor timer scheduled/fired/cancelled/stale | ActorRuntime | 유지 |

외부 `ServerMetricsSnapshot`의 필드명 변경이 필요하지 않다면 기존 aggregate shape를 유지하고 composition root에서 Binding/Handler snapshot을 합친다. 이름 변경은 별도 API 변경으로 취급한다.

## 11. 구현 단계와 단계별 종료 조건

### 0단계 — Baseline과 characterization

- 현재 dirty worktree를 기록하고 관련 변경을 되돌리지 않는다.
- 기존 전체 CTest와 주요 integration test 결과를 baseline으로 남긴다.
- protocol sink/mapper의 byte output, effect 순서, command terminal/admission metric을 characterization test로 고정한다.
- `GameServer` composition의 현재 dependency와 thread 호출 위치를 문서화한다.

종료 조건: 이후 단계에서 wire/effect/metric 회귀를 비교할 golden test가 있다.

### 1단계 — 계약과 pure encoder 도입

- `HandlesOutcome`, effect context, domain별 outcome 타입을 추가한다.
- 기존 mapper/sink 로직에서 pure `encode_*()`를 추출한다.
- 기존 sink는 일시적으로 encoder를 호출하는 단방향 compatibility adapter로 유지한다.
- 모든 encoder의 exact byte test를 추가한다.

종료 조건: 외부 동작은 그대로이고 encoding policy와 delivery policy가 분리돼 있다.

### 2단계 — Domain fact와 invariant 완결

- `ZoneResult::empty`, `RoomResult::participant_count`를 추가한다.
- 모든 Domain 반환 경로가 post-state fact를 채우도록 단일 result 생성 경로를 정리한다.
- terminal/empty와 timer intent의 금지 조합을 validation/test로 고정한다.
- 기존 Binding 재조회 결과와 새 fact가 일치하는 characterization test를 거친 뒤 재조회를 제거한다.

종료 조건: Handler가 Domain 재조회 없이 disposition을 결정할 수 있다.

### 3단계 — Typed tell/self timer

- `ActorContext`와 `ActorRuntime`에 typed facade/private raw bridge를 추가한다.
- `translateTell()`을 단일 Binding 선택/변환 경로로 만든다.
- Zone/Room/Player Binding의 `makeTell()` 지원 메시지를 완결한다.
- TimerEntry를 internal `TellPayload` carrier 기반으로 전환한다.
- schedule/fire/cancel/purge/stale/exception의 slot accounting test를 통과시킨다.
- 모든 Handler/Binding에서 직접 `TellPayload::of()`와 `trySchedule(ActorSubmission)` 호출을 제거한다.

종료 조건: 즉시 tell과 timer fire가 같은 translation 규칙을 쓰며 timer가 capacity를 이중 예약하지 않는다.

### 4단계 — Zone vertical slice

- `ZoneOutcomeHandler`와 Zone effect context를 구현한다.
- transition/client/timer/disposition 실행을 Binding callback에서 Handler로 옮긴다.
- `ZoneActorBindingConfig::on_result`를 제거한다.
- direct request와 handoff completion 양쪽의 compile-time contract/effect order를 검증한다.

종료 조건: Zone Binding은 Domain Result 필드를 읽지 않고 Handler 반환 disposition만 Runtime에 전달한다.

### 5단계 — Room vertical slice

- `RoomOutcomeHandler`와 Room runtime/workflow outcome overload를 구현한다.
- deadline 선예약 실패를 `RoomRuntimeOverloaded`로 분리한다.
- deadline 유지/취소, tick, reply/fanout, return, grant, disposition을 Handler로 이동한다.
- Room effect metric을 Handler로 옮기고 외부 snapshot 의미를 보존한다.
- `RoomActorBindingConfig::on_result`를 제거한다.

종료 조건: fake `RoomResult` 없이 pre-domain failure가 종결되고 terminal/empty Room이 timer와 함께 passivate된다.

### 6단계 — Player vertical slice

- `PlayerSnapshotAccess`와 `PlayerOutcomeHandler`를 구현한다.
- room join tell, snapshot enqueue/rollback/retry, outbound reservation/commit을 Handler coroutine으로 이동한다.
- ActorState는 Handler task handle만 보관하고 call-local pending state는 coroutine frame으로 이동한다.
- Domain 전 load와 connection-close save는 Binding에 유지한다.
- 두 Player actor가 같은 Handler에서 동시에 suspend/resume하는 테스트를 추가한다.

종료 조건: Player post-domain effect 분기가 Binding에서 제거되고 기존 batch atomicity/suspend semantics가 유지된다.

### 7단계 — Workflow outcome 배출과 Service 도입

- 기존 `ZoneHandoffService`를 `ZoneHandoffWorkflow`로, `RoomEntryService`를 `RoomEntryWorkflow`로 분리/개명한다.
- Workflow의 sink/Handler dependency를 제거하고 invalid 조합을 표현할 수 없는 `WorkflowStartResult` variant 및 bounded `drain()` emission으로 교체한다.
- `PlayerService`, `ZoneService`, `RoomService`를 추가한다.
- `ZoneConnectionCleanup`/`RoomConnectionCleanup` port를 추가하고 `PlayerService`의 connection-wide close 순서와 retry idempotency를 고정한다.
- Gateway의 session/route/rollback/workflow 선택을 Service로 옮긴다.
- direct actor admission과 post-accept workflow failure의 종료 경로를 분리해 테스트한다.
- Service `forward()`의 RAII guard가 Handler 정상 반환과 예외 unwind 모두에서 command lifecycle token을 정확히 한 번 release하는지 검증한다.

종료 조건: Workflow는 effect layer를 모르고 Service가 모든 workflow outcome을 Handler에 전달한다.

### 8단계 — Gateway 축소와 legacy 제거

- Gateway를 decode + typed Service dispatch로 축소한다.
- `MessageDispatcher`, `CommandRouter`, `RoutedCommandIngress`, `RoutedCommand`를 call site가 0인 순서대로 제거한다.
- `ProtocolPlayerResponseSink`, `ProtocolZoneResultSink`, `ProtocolRoomResultSink`, `ProtocolResponseMapper`를 제거한다.
- CMake source/test 목록과 `GameServer` 생성 순서, stats wiring, shutdown wiring을 정리한다.

종료 조건: legacy adapter가 없고 입력/출력 합류점이 각각 Service/OutcomeHandler 하나뿐이다.

### 9단계 — 문서와 최종 검증

- `server-architecture-draft.md`, actor messaging/timer/lifecycle 문서를 새 책임과 API로 갱신한다.
- Linux debug build, 전체 CTest, ASan/UBSan, TSan을 실행한다.
- load/integration test로 queue saturation, shutdown drain/cancel, transition retry를 검증한다.

종료 조건: 아래 완료 체크리스트가 모두 충족된다.

## 12. 검증 매트릭스

### Compile-time

- 모든 Handler/context/outcome/return 조합의 `HandlesOutcome` `static_assert`
- unsupported message를 Binding에 추가하지 않으면 typed tell test가 실패
- game target이 runtime/server symbol을 참조하지 않는 기존 layer check 유지

### Domain

- Zone 마지막 player 이탈: `empty == true`, `tick_after == nullopt`
- Zone 첫 player 진입과 tick: post-state `empty`와 next tick 일치
- Room waiting/running에서 마지막 participant 이탈
- Room Cleared/Failed: terminal phase, no tick/deadline intent
- outcome/failure_reason/grants 조합 invariant

### Runtime timer/tell

- 즉시/지연 tell의 동일 `translateTell()` 사용
- schedule capacity rejection 시 timer/slot 미생성
- cancel, passivation purge, shutdown purge의 정확한 slot 반환
- stale incarnation discard가 actor를 재활성화하지 않음
- fire 후 mailbox enqueue에서 capacity 이중 예약 없음
- TimerEntry 삽입/translation 예외 시 slot leak 없음
- Zone/Room/Player의 모든 지원 메시지와 target identity validation

### Handler

- domain별 effect 순서
- Result fact → disposition 변환
- Room pre-domain overload without fake Result
- Player snapshot transaction rollback/retry
- outbound immediate reservation과 suspend/resume
- 같은 Handler instance의 reactor/여러 Worker 동시 호출
- 두 suspended Player outcome 사이 state isolation
- send failure, transition publish failure, shutdown cancellation 정책

### Workflow/Service

- immediate terminal emission과 later `drain()` emission의 exactly-once forwarding
- `WorkflowRejected`는 state/token 없음, `WorkflowCompleted`는 drain 재출력 없음, `WorkflowPending`은 정상 drain 또는 shutdown cancel 중 정확히 한 terminal 경로만 사용
- handoff/entry/return rollback과 compensation
- stale completion과 disconnect cleanup
- first admission `Full`/`Closed`는 `FramePostResult`로 반환
- post-accept stage failure는 typed outcome으로 Handler에 전달
- session attach/route mutation rollback
- Service RAII forwarding에서 Handler 반환/예외 후 command lifecycle exactly-once 및 terminal/rejection metric 보존
- connection close의 pending workflow → Zone → Room → Player 순서와 `Full` 재시도 idempotency

### Protocol/Integration

- malformed frame, unknown `MessageType`, invalid payload는 Handler를 거치지 않고 protocol policy 적용
- 모든 valid message가 정확한 typed Service method로 dispatch
- 기존 TCP wire bytes와 request ID 보존
- Actor FIFO, Player response batch atomicity, Room fanout 순서 보존
- graceful drain/cancel 후 timer, reservation, transition ticket, command token 잔존 없음

## 13. 최종 완료 체크리스트

- [ ] 공통 virtual OutcomeHandler 기반 클래스가 없다.
- [ ] 모든 Handler 경로가 `HandlesOutcome` concept를 만족한다.
- [ ] Handler는 호출 간 mutable execution state를 보관하지 않는다.
- [ ] 공유 Handler와 dependency의 thread-safety 계약이 header에 명시돼 있다.
- [ ] connection/request identity가 context와 outcome에 중복되지 않는다.
- [ ] Domain Result는 runtime disposition 대신 post-state Domain fact를 반환한다.
- [ ] `empty/terminal`과 timer intent의 모순 조합이 생성되지 않는다.
- [ ] Binding은 생성된 Domain Result 필드를 읽거나 Domain을 재조회하지 않는다.
- [ ] Handler API는 전체 Actor 객체를 받지 않는다.
- [ ] Player snapshot capability만 persistence transaction용 예외로 허용한다.
- [ ] runtime/admission/workflow 실패를 fake Domain Result로 표현하지 않는다.
- [ ] `ActorContext`의 typed facade와 private virtual bridge 형태가 구현돼 있다.
- [ ] Handler는 `TellPayload`나 `ActorSubmission`을 직접 만들지 않는다.
- [ ] 즉시 tell과 지연 tell은 동일한 `translateTell()` 규칙을 사용한다.
- [ ] timer slot은 schedule부터 terminal까지 정확히 한 번 예약/반환된다.
- [ ] Workflow는 Handler/Encoder/OutboundSink를 참조하지 않는다.
- [ ] `WorkflowStartResult`는 rejected/completed/pending의 모순된 조합을 표현할 수 없다.
- [ ] Service가 immediate/later workflow outcome을 정확히 한 번 Handler에 전달한다.
- [ ] Service가 Workflow emission의 `CommandReleaseToken`을 Handler 호출 scope 동안 RAII로 소유하고 exactly once release한다.
- [ ] `PlayerService`의 Zone/Room cleanup dependency가 좁은 port로 명시되고 close retry가 idempotent하다.
- [ ] Gateway는 application route/workflow를 판단하지 않는다.
- [ ] protocol-level failure와 application outcome 경계가 테스트로 구분돼 있다.
- [ ] client-visible application/domain outcome은 모두 OutcomeHandler를 거친다.
- [ ] wire format, Actor FIFO, admission 정책, effect 순서와 통계 의미가 유지된다.
- [ ] legacy sink/mapper/router/dispatcher call site와 CMake 항목이 제거됐다.
- [ ] Linux debug, 전체 CTest, ASan/UBSan, TSan이 통과한다.
- [ ] 현재 작업 트리의 기존 변경을 되돌리지 않았다.
