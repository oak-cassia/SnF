# Actor 간 메시지와 게임 시간 결정

> 상태: ①② 채택·구현, ③ 채택·미구현
> 범위: Actor가 다른 Actor에게 보내는 메시지, 후속 작업의 목적지 분기, 콘텐츠가 시간을
> 세는 기준

## 1. 결정

세 줄로 요약된다.

1. Actor끼리 메시지는 보내되 서로 기다리지 않는다.
2. Actor 위치와 mailbox 라우팅은 Runtime이 책임진다.
3. 게임 시간은 tick 수가 아니라 실제 경과 시간으로 계산한다.

이 결정은 협동 Battle Room이 `PartyActor → Room`, `Room → PlayerActor` 흐름을 필요로 하면서
나왔다. 그전까지 Actor 간
상호작용은 result를 reactor로 올려 `ProtocolGateway`와 `CommandRouter`를 거쳐 다시
내려와야 했고, 그 결과 조율 코드가 도메인 코드보다 세 배 이상 많았다.

## 2. Actor 간 메시지

### 2.1 비동기 전송은 허용, 동기 대기는 금지

```text
허용:  Room → Player : 보상 처리해        (보내고 끝)
금지:  Room → Player : 정보 줘
       Room ← Player : 응답 올 때까지 대기
```

동기 대기가 **불가능**한 것은 아니다. `AsyncOperationAwaiter`가 있고 PlayerActor는 이미
repository를 `co_await`한다. 그러므로 이것은 능력의 한계가 아니라 **런타임 계약**이다.

금지하는 이유는 순환 대기다. A가 B를, B가 A를 기다리면 둘 다 영구히 `Suspended`로 남고,
`ActorRuntimeDrained`의 `no running or suspended task` 항이 영원히 거짓이 되어 서버가
종료되지 않는다. **현재 런타임에는 이 순환을 탐지하거나 해소하는 장치가 없으므로,
동기 대기를 허용하지 않는 것으로 문제 자체를 제거한다.** cycle detection이나 deadline
기반 강제 해제를 도입한다면 이 계약을 다시 검토한다.

계약은 타입으로 강제된다.

```cpp
[[nodiscard]] PostResult tryTell(ActorKey target, TellPayload payload);
```

`PostResult`는 awaitable이 아니다. 기다리고 싶어도 기다릴 수단이 없다.

### 2.2 Domain Event는 다중 소비가 실제로 생길 때

Actor 간 tell은 1:1이다. 하나의 사실을 여러 곳이 받아야 할 때 — 예를 들어 `BattleCleared`를
보상·업적·통계가 함께 소비할 때 — 송신자가 수신자 전부를 알아야 하므로 결합이 생긴다.
그때 Domain Event를 도입한다.

**Battle Room 1차에서 같은 사실을 두 곳 이상이 소비하는지 확인한 뒤에 만든다.** 도입할
때는 두 가지를 먼저 정한다.

- 구독을 누가 소유하는가. reactor가 소유하면 지금 벗어나려던 결합으로 되돌아간다.
- 보장 수준. 구독자 N명이면 admission이 N번이고 각각 `Full`이 날 수 있다. **best-effort와
  구독자별 실패 카운트**를 기본으로 하고, 보장 전달은 영속화와 재시도를 요구하는 별개
  요구사항으로 다룬다.

### 2.3 submission은 대상 Binding이 조립한다

송신자 Binding은 대상 Actor의 command 타입을 알지 않는다. 런타임이 이를 강제한다.

```cpp
ActorBinding::makeSubmission:  target.kind != kind()        → throw
ActorRuntime::tryPost:         _bindings[kind] != _binding  → throw
```

그래서 송신자는 도메인 payload만 넘기고, 대상 kind에 등록된 Binding이 자기 타입으로
복원해 submission을 만든다.

```text
Actor A
  → Result의 게임 필드 (예: RoomResult::grants)
  → A의 Binding이 대상과 payload로 번역
  → ActorContext::tryTell(target, payload)
  → ActorRuntime: _bindings[target.kind]->makeTell(target, payload)
  → 기존 tryPost 경로 (해시 → worker → ingress → mailbox)
  → Actor B
```

`TellPayload`는 move-only다. 전달된 tell을 두 번 claim할 수 없다는 뜻이며, 그 결과
`ZoneResult`처럼 tell을 품는 result도 복사 불가가 된다.

payload 타입이 맞지 않으면 `makeTell`이 `std::nullopt`를 돌려주고 런타임은
`std::logic_error`를 던진다. `payloadAs` 불일치와 같은 배선 버그로 취급하며, 런타임
조건이 아니다.

### 2.4 새 큐를 만들지 않는다

`tryTell`은 기존 `tryPost` 경로를 그대로 쓴다. 그래서 reactor가 보낸 명령과 Actor가 보낸
명령의 ingress·mailbox 의미가 동일하다. 상한, 포화 정책, FIFO 보장, drain 판정이 송신자에
따라 갈리지 않는다.

FIFO는 **한 송신자가 한 turn에 보낸 tell 사이에서만** 보장된다. 서로 다른 Worker의 두
송신자가 보낸 tell의 상대 순서는 보장 대상이 아니다.

### 2.5 tell은 ActivateIfMissing을 쓴다

`ExistingOnly`였다면 대상이 passivate된 사이에 도착한 tell이 `Accepted`를 받고도 조용히
버려진다. 보상 지급 같은 tell은 대상이 잠들어 있어도 도착해야 하므로 활성화한다.

### 2.6 makeTell은 thread-safe여야 한다

다른 submission factory와 달리 `makeTell`은 **모든 Worker에서 동시에** 호출된다.
`makeCommand`는 reactor 한 스레드에서만 불리므로 이 제약이 없었다.

구현은 읽기 전용 변환으로 유지한다. 캐시, 시퀀스 카운터, 임시 버퍼 같은 가변 상태를 넣지
않는다. TSan이 이를 검증한다.

## 3. 후속 작업의 목적지 분기

Binding이 분기한다. Sink는 네트워크 출력만 담당한다.

```text
                    Actor
                      ↓  Result (게임 의미만)
                   Binding
                  /       \
        PlayerResponseSink   ActorContext
             ↓                /        \
      OutboundChannel   tryTell    trySchedule
        (서버 자원)          ↓            ↓
                        다른 Actor    timer heap
                            (런타임 자원)
```

두 다리가 `ActorContext`를 통하고 한 다리는 통하지 않는 것은 우연이 아니다. mailbox와
timer heap은 런타임이 소유한 자원이고, 소켓은 아니다. `ActorRuntime`은 frame이 존재한다는
사실조차 모른다.

### 3.1 Result는 게임 의미만 담는다

한때 모든 Actor가 공유하는 `FollowUpAction` variant가 있었고, 그 안에 `ActorKey`와
`TellPayload`가 들어 있었다. 지금은 없다. **런타임 타입을 도메인 result에서 걷어내고,
각 result가 자기 언어로 말하게 했다.**

```cpp
struct RoomResult
{
    ...
    std::optional<std::chrono::milliseconds> complete_after;   // 이 전투에 남은 시간
    std::vector<StreetExperienceGrant> grants;                 // 누구에게 얼마
};

struct ZoneResult
{
    ...
    std::optional<std::chrono::milliseconds> tick_after;       // 다음 tick까지
};
```

Binding이 번역한다. `complete_after`/`tick_after` → `trySchedule`, `grants` → `tryTell`.

바꾼 이유는 계층 규칙이 아니라 눈에 보이는 비용이었다.

- `TellPayload`가 move-only라 **result 전체가 복사 불가**가 됐다. `ZoneResult`를 담아두려던
  테스트 recorder를 필드 하나만 담도록 고쳐야 했다.
- 보상을 단언하려면 `payload.take<T>()`로 꺼내야 했는데, **그 호출이 result를 변형한다.**
  단언이 상태를 바꾸는 테스트가 나온다.
- 지금은 값 비교로 끝난다: `result.grants == std::vector{{player, 300}}`.

도메인이 런타임 목적지를 이름 붙이지 않으므로 `snf/runtime` 의존도 함께 사라진다.

응답도 마찬가지로 각 result의 채널이다 — `PlayerResult::responses`. 응답 타입은 Actor
종류마다 다르고, Zone에는 대응하는 response 타입이 아예 없다.

Binding이 다른 Binding의 ingress를 직접 들고 있지 않다는 점이 핵심이다. 그래야
`PlayerBinding → RoomBinding → PartyBinding` 같은 참조 그래프가 생기지 않고, actor
routing이 런타임의 책임으로 남는다.

tell을 `requiredSlots`에 넣지 않는다. 네트워크 backpressure와 mailbox 포화는 다른
자원이고, 섞으면 sink의 스칼라 가격 계산이 다시 깨진다.

## 4. 게임 시간

> 런타임 전달은 구현됐다(`ActorContext::observedAt`). 이를 소비하는 콘텐츠는 Room의
> 실제 전투에서 시작된다.

### 4.1 tick 수로 세지 않는다

```text
❌ 3600번 Tick이 오면 보스 격노
✅ 전투 시작 180초 후 보스 격노
```

`TimerRequest{delay}`는 fixed-delay다. 핸들러가 결과를 반환한 시점부터 delay를 재므로
실제 주기는 `interval + 핸들러 실행 + mailbox 대기`이며 정확한 20 Hz가 아니다. 게임
로직이 tick을 세면 이 drift가 그대로 밸런스 오차가 된다.

지속시간, 쿨다운, 타임아웃을 전부 실경과 시간으로 정의하면 drift는 무해해진다. **그
결과 런타임에 fixed-rate timer를 추가할 이유도 사라진다.**

### 4.2 Actor는 시계를 직접 읽지 않는다

`ZoneActor`에는 시계가 없다. `handle()`이 명령만 받고 결과만 돌려주는 순수 함수라
테스트가 결정론적이다. `Room`이 `steady_clock::now()`를 직접 부르면 그 성질이 깨지고,
방금 제거한 `TimerClock` 주입을 도메인 Actor에 되돌리는 셈이 된다.

대신 런타임이 **명령을 처리하기 시작한 시점**을 알려 준다.

```cpp
[[nodiscard]] virtual std::chrono::steady_clock::time_point observedAt() const noexcept;   // ActorContext
```

한때 이 값을 명령 안에 담는 형태(`struct Tick { observed_at; }`)로 적어뒀는데, **명령은 예약
시점에 Binding이 만들고 발화는 나중이다.** 그러면 담긴 시각이 낡는다. 그래서 `ActorContext`가
turn마다 알려주고, Binding이 dispatch 시점에 도메인 핸들러로 넘긴다. `steady_clock`은
표준 타입이므로 게임 계층이 런타임을 참조하게 되지 않는다.

Actor는 시점 산술만 한다.

```cpp
if (command.observed_at >= _enrage_deadline) { enrage(); }
```

### 4.3 observed_at은 만료 시각이 아니라 처리 시작 시각이다

이 구분이 핵심이다. timer heap에서 알람을 꺼낸 시각을 실어 주면, mailbox에서 500 ms 밀린
Actor는 500 ms 전의 시간을 보게 되어 애초에 없애려던 문제가 남는다.

```text
deadline    = 10:00:05.000
실제 처리    = 10:00:05.500
observed_at = 10:00:05.500   → 즉시 만료 판정
```

런타임에는 이미 이 시각이 있다. `runReadyActorTurn`이 mailbox에서 명령을 꺼내 dispatch
하기 직전에 `queue_wait` 메트릭용으로 `steady_clock::now()`를 뜨고 버린다. 이를 turn당 한
번 떠서 `ActorContext`로 노출하면 된다.

### 4.4 rate 기반 계산의 dt도 observed_at 차이로

```text
❌ Tick마다 10 데미지
✅ 초당 200 데미지 × (observed_at - _last_observed_at)
```

Actor는 직전 관측 시각도 상태로 갖는다. 첫 tick에는 전투 시작 시각을 초기값으로 쓴다. 이
규칙을 지키지 않으면 deadline은 실시간을 따라가는데 도트 데미지는 tick 수를 따라가서
서버가 밀릴 때 어긋난다.

## 5. 결과와 trade-off

Room은 "메시지를 만들고 상태를 바꾸는 것"에만 집중하고, 네트워크·라우팅·타이머 같은 실행
환경 문제는 런타임으로 빠진다. `ProtocolGateway`가 Room 조율 코드로 다시 비대해지지 않는다.

대신 치르는 것이 있다.

- 컴파일 타임 타입 검사 일부가 런타임 검사로 내려간다. payload 불일치는 `logic_error`로
  드러나며, 조용히 잘못된 명령이 되지는 않는다.
- `makeTell` thread-safety가 새 계약으로 생긴다. 지금 구현들은 const에 무상태라 만족하나,
  가변 상태를 넣으면 깨진다.
- tell을 품는 result는 복사 불가가 된다.
- 워커 간 tell이 늘면 `ActorRuntime::tryPost`의 전역 `_state_mutex`가 경합 지점이 된다.
  지금은 post가 사실상 reactor 한 스레드에서만 오므로 경합이 없다. **측정한 뒤에**
  판단하며, tell 도입과 락 구조 변경을 같은 시기에 하지 않는다.

## 6. 관측과 검증

- 워커 간 tell이 정상 전달되는지 (같은 Worker, 다른 Worker)
- 대상 mailbox 포화 시 `Full`이 반환되고 무한 증가가 없는지
- 한 송신자가 한 turn에 보낸 tell이 FIFO로 도착하는지
- 미상주 대상이 활성화되어 tell을 받는지
- close 이후 tell이 `Closed`로 거부되는지
- TSan에서 `makeTell` 동시 호출에 race가 없는지
- `_state_mutex` 경합: 워커 간 tell 트래픽이 생긴 뒤 reactor turn과 post 지연 분포
- fixed-delay drift와, `observed_at` 도입 후 게임 시간이 tick 지연에 끌려가지 않는지
