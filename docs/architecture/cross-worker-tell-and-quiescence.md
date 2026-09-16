# Cross-worker Tell과 Publication-safe Quiescence

> 분류: **구현 해설 / 비규범 문서**
> 구현 상태: **6단계 완료**
> 기준 계약: [Unified Worker Runtime](./unified-worker-runtime.md)
> 관련 로드맵: [6단계 — Cross-worker event와 Public Tell](../development-roadmap.md)

6단계는 Worker가 여러 개일 때 Actor끼리 안전하게 메시지를 주고받고, 서버 종료 중에도 이미
받아들인 메시지를 잃지 않도록 만든 단계다.

핵심은 두 부분으로 나뉜다.

```text
6A: 다른 Worker가 소유한 Actor에게 tell을 전달한다.
6B: shutdown 중 모든 Actor 작업이 끝났는지 publication race 없이 판정한다.
```

6A가 기능을 추가한다면, 6B는 그 기능 때문에 생긴 shutdown race를 막는 안전장치다. 이번 단계에서
shutdown 전체를 완성한 것은 아니다. Cross-worker Actor 도입에 필요한 최소 loss-prevention만
10단계에서 앞당겼으며 watchdog, 장시간 shutdown 부하와 운영 품질 검증은 10단계에 남아 있다.

## 1. 다른 Worker의 Actor에게 tell하기

같은 Worker에 있는 Actor끼리의 전달은 owner thread 안에서 끝난다.

```text
Worker 0
├─ Player A
└─ Player B

Player A
   └─ tell ─→ Player B mailbox
```

대상이 다른 Worker에 있다면 source Worker가 target Worker의 inbox에 concrete event를 publish한다.

```text
Worker 0                                  Worker 1
Player A                                  Room B
   │                                         ▲
   └─ TellActorEffect                        │
      → Worker::tellInternal()               │
      → WorkerInbox source lane              │
      → RemoteActorMessage ──────────────────┘
                                → Room B mailbox
```

전체 분기는 다음과 같다.

```text
Actor
  ↓ TellActorEffect
Worker::tellInternal(target)
  ↓
target owner가 현재 Worker인가?
  ├─ Yes → local Actor mailbox에 admission
  └─ No  → target WorkerInbox에 RemoteActorMessage publish
             ↓ target Worker가 inbox drain
          local Actor mailbox에 admission
```

Local tell도 handler를 직접 호출하지 않는다. 같은 Worker에서는 inbox를 우회할 뿐, 항상 target Actor의
mailbox에 넣어 non-reentrancy와 FIFO를 유지한다. Remote tell은 target Worker의 source별 bounded SPSC
lane을 사용하며 queue 자리가 날 때까지 Worker thread를 막지 않는다.

구현의 중심 타입과 경로는 다음 파일에 있다.

- [`ActorEnvelope`](../../include/snf/worker/actor_envelope.hpp): Actor message와 byte charge를 운반하는 공통 타입
- [`RemoteActorMessage`](../../include/snf/worker/worker_event.hpp): target `ActorKey`와 `ActorEnvelope`를 담는 concrete event
- [`Worker::tell()`과 수신 처리](../../src/worker/worker.cpp): local/remote routing, admission과 terminal metric 처리
- [`WorkerInbox`](../../include/snf/worker/inbox.hpp): source별 bounded SPSC lane

### ActorEnvelope을 독립시킨 이유

`ActorEnvelope`이 `actor.hpp` 안에 있으면 Actor event가 envelope을 사용하려는 순간 다음과 같은 순환
의존성이 생길 수 있다.

```text
actor.hpp
   ↓
worker_event.hpp
   ↓
actor.hpp
```

공통 타입을 더 작은 헤더로 분리하면 두 상위 타입이 서로를 include하지 않아도 된다.

```text
       actor_envelope.hpp
          ↑          ↑
     actor.hpp   worker_event.hpp
```

## 2. Accepted의 정확한 의미

`Worker::tell(target, message)`가 `Accepted`를 반환하더라도 local과 remote의 보장 범위는 다르다.

```text
local Accepted
= target Actor mailbox admission 완료

remote Accepted
= target WorkerInbox admission 완료
```

Remote Accepted는 target Actor가 메시지를 실행했거나 mailbox admission까지 끝났다는 뜻이 아니다.

```text
source Worker
   ↓ tryPush: Accepted
target WorkerInbox
   ↓ drain
target Actor mailbox admission
   ↓
Actor turn 실행
```

따라서 remote 전달에는 두 종류의 terminal failure가 있다.

1. Source admission 실패

   - inbox lane full
   - inbox closed 또는 target port 미배선
   - event charge가 `uint32_t` 범위를 초과

2. Target delivery 실패

   - Actor runtime 없음
   - owner 재검증 실패로 인한 misroute
   - target mailbox 또는 activation admission 실패

Metric도 이 경계를 따라 나뉜다.

```text
source remote routing 시도 = remote_tells_sent + remote_tell_rejections

remote_tells_received
  = remote_tells_delivered
  + remote_tell_delivery_failures
```

첫 번째 식의 범위는 public Running gate를 통과해 remote owner로 routing된 시도다. Local tell이나
shutdown 진입 후 public gate에서 즉시 닫힌 tell은 포함하지 않는다. Target의 reason metric인
`misrouted_actor_events`, `actor_events_without_runtime` 등은
`remote_tell_delivery_failures`의 부분집합이다.

정상적으로 group shutdown까지 끝났다면 전체 Worker에 대해 다음 보존식도 만족해야 한다.

```text
Σ remote_tells_sent == Σ remote_tells_received
```

## 3. Cross-worker tell이 만드는 shutdown race

Worker별 empty 상태만 모으면 종료를 너무 일찍 확정할 수 있다.

```text
Target: Waiting, quiescent bit = 1

Source: target을 active로 표시
Target: inbox empty 확인
Source: 아직 enqueue 전
Target: 다시 quiescent 표시
Source: enqueue 성공
```

이제 target inbox에는 event가 있지만 target bit는 quiescent일 수 있다. Enqueue 뒤에 target을 다시
active로 표시하는 것만으로도 충분하지 않다.

```text
Target: inbox empty 확인
Source: enqueue
Source: target active 표시
Target: 앞서 관측한 empty를 근거로 quiescent commit
```

문제의 본질은 target이 publication 이전에 관측한 empty 결과를 publication 이후에도 사용할 수 있다는
점이다. 단순 active bit만으로는 stale empty 관측을 구분할 수 없다.

## 4. Publication epoch와 conditional commit

[`WorkerQuiescenceBarrier`](../../include/snf/worker/barrier.hpp)는 성공한 publication의 세대를 나타내는
epoch를 보관한다. Worker는 local work를 검사하기 전에 snapshot을 읽고, 검사가 끝난 뒤 epoch가 그대로일
때만 자신의 quiescent bit를 세운다.

```text
Target: snapshot epoch = 100
Target: inbox / ready Actor / blocked Actor 검사

Source: target inbox에 enqueue 성공
Source: notePublished(target)
        epoch 100 → 101

Target: tryMarkQuiescent(me, expected=100)
        current epoch=101
        → 실패하고 다시 검사
```

Barrier의 핵심 연산은 다음과 같다.

```cpp
Snapshot snapshot() const noexcept;
void notePublished(WorkerId target) noexcept;
bool tryMarkQuiescent(WorkerId me, std::uint32_t observed_epoch) noexcept;
void markActive(WorkerId me) noexcept;
```

- `snapshot()`은 armed, aborted, all-quiescent와 현재 epoch를 함께 읽는다.
- `notePublished()`는 target bit를 active로 만들고 publication epoch를 증가시킨다.
- `tryMarkQuiescent()`는 관측한 epoch가 현재 epoch와 같을 때만 caller bit를 quiescent로 만든다.
- `markActive()`는 Waiting 상태의 Worker가 work를 발견했을 때 자신의 bit를 active로 되돌린다.

Target이 이미 active여도 publication이 성공하면 epoch를 반드시 증가시킨다. Epoch의 의미는 “target이
active인가?”가 아니라 “내 empty scan 이후 새로운 publication이 있었는가?”이기 때문이다.

### 64-bit state layout

Barrier state는 하나의 64-bit atomic에 저장한다.

```text
bits  0..31 : Worker quiescent mask
bits 32..61 : 30-bit publication epoch
bit      62 : armed
bit      63 : aborted
```

따라서 WorkerGroup participant는 1개 이상 32개 이하로 제한된다. Shutdown 한 번에 30-bit epoch를
모두 소진하면 wrap하지 않고 barrier를 abort하여 forced cleanup으로 전환한다. Epoch가 재사용되어 오래된
empty 관측과 새 publication을 혼동하는 상황을 허용하지 않기 위해서다.

## 5. Publish 순서와 Worker publisher 전제

Remote event publication 순서는 다음과 같다.

```text
tryPush(event)
  ↓ Accepted
notePublished(target)
  ↓
caller에게 Accepted 반환
```

Queue publication과 `notePublished()` 사이에는 짧은 gap이 있다. 이 gap이 안전한 이유는 publisher인
source Worker가 아직 active participant이기 때문이다.

```text
Source Worker가 publish 중
  ↓
target queue에는 event가 들어갔지만 notePublished 전
  ↓
source bit가 active이므로 all-quiescent 성립 불가
```

이 논리에는 중요한 범위 제한이 있다.

> Armed barrier 동안 quiescence에 영향을 주는 cross-worker event의 producer는 같은
> WorkerGroup의 participant Worker thread여야 한다.

Foreign thread가 publish할 수 있으면 모든 Worker가 quiescent인 상태에서 enqueue와
`notePublished()` 사이의 gap이 노출된다. Foreign-thread producer를 추가하는 단계에서는 producer
participation counter 또는 별도 in-flight publication protocol이 필요하다.

## 6. Shutdown phase와 ingress 차단

현재 구현을 개념적인 lifecycle로 표현하면 다음과 같다. 이것은 별도 public enum 계약이 아니라 shutdown
코드의 책임 경계를 설명하기 위한 이름이다.

```text
Running
  ↓
QuiescingActors
  ↓
DrainingConnections
  ↓
HardCleanup
```

`Running`에서는 public tell과 network request를 받는다. `QuiescingActors`에 들어가면 새로운 public
ingress는 막지만, 이미 accepted된 Actor turn에서 파생되는 내부 tell과 inbox event는 계속 처리한다.

```text
public Worker::tell()       → Running에서만 허용
TellActorEffect internal tell
                            → QuiescingActors까지 허용
RemoteActorMessage         → QuiescingActors까지 처리
```

서버 종료와 동시에 모든 tell을 막으면 이미 실행 중인 Actor가 후속 Actor에게 결과를 전달하지 못하고 논리
중간에서 끊길 수 있다.

Network ingress는 Actor quiescence보다 먼저 닫는다.

```text
1. shutdown flag와 network stopping 설정
2. listener poll registration 제거
3. connection EPOLLIN 비활성화
4. 이미 예약된 read work 제거
5. stale readable PollEvent가 새 read work를 만들지 못하게 차단
6. Actor quiescence 시작
```

`EPOLLOUT`은 유지한다. 이미 처리한 결과가 connection write buffer에 있다면 Actor quiescence 이후의
connection drain에서 끝까지 flush해야 하기 때문이다. 실제 구현은
[`Worker::beginShutdownPhaseA()`](../../src/worker/worker.cpp)에 있다.

## 7. Active와 Waiting 상태 머신

Barrier에 참여하는 Worker는 shutdown 중 `Active`와 `Waiting`을 반복한다.

```text
Active
  ↓
inbox drain / due timer / ready Actor / write flush
  ↓
runnable 또는 blocked Actor work가 남았는가?
  ├─ Yes → Active 유지
  └─ No  → Waiting
```

Waiting에서 work를 발견하면 application handler를 실행하기 전에 active를 publish해야 한다.

```text
readiness 또는 work presence 관측
  ↓
markActive(me)
  ↓
Active 전환
  ↓
application dispatch
```

Work를 찾지 못하면 snapshot epoch를 기준으로 local empty 상태를 다시 확인하고 conditional commit을
시도한다.

```text
snapshot S
  ↓
inbox / ready Actor / blocked Actor 검사
  ↓
tryMarkQuiescent(me, S.epoch)
  ├─ 실패 → publication이 있었으므로 다시 검사
  └─ 성공 → snapshot으로 group 상태 확인
```

`tryMarkQuiescent()` 성공 후 `all_quiescent`가 보이면 같은 epoch의 snapshot을 즉시 한 번 더 읽어
armed·aborted·all-quiescent 상태가 그대로인지 검증한다. 이것은 application work를 처리하는 추가 loop가
아니라 연속 atomic snapshot 검증이다.

## 8. Timer와 blocked Actor

6단계의 `TimerPayload`는 `AwaitTimeout`과 `ConnectionCloseDeadline` 두 종류다.

- 현재 operation을 기다리는 blocked Actor는 quiescent가 아니다. Shutdown은 먼저 logical cancel을
  전달하고 coroutine이 cancel 결과를 처리하게 한다.
- Logical cancel 뒤 heap에 남은 `AwaitTimeout`이 현재 `AwaitKey`와 일치하지 않으면 stale entry다.
  실제 Actor work가 아니므로 quiescence를 막지 않는다.
- `ConnectionCloseDeadline`은 Actor quiescence가 아니라 `DrainingConnections`의 책임이다.

일반 application timer가 추가되면 별도 정책이 필요하다. Shutdown 진입 시 logical cancel하거나, live
timer가 남아 있는 동안 quiescence를 허용하지 않는 방식 중 하나를 명시해야 한다. Stale timer를 무시하는
현재 규칙을 live application timer에 그대로 적용하면 안 된다.

## 9. 검증하는 불변식

Stage 6 회귀 테스트는 다음 상황을 직접 만든다.

- Publication 사이에 epoch가 바뀌면 stale `tryMarkQuiescent()`가 실패한다.
- Target이 이미 active여도 `notePublished()`가 epoch를 증가시킨다.
- Barrier가 armed된 뒤 source Actor가 remote tell을 만들고 target Actor가 이를 처리한다.
- 두 Worker에 실제 suspended Actor를 만들고 shutdown logical cancel이 모두에게 전달된다.
- Shutdown Phase A 이전에 만들어진 stale readable event가 request ingress를 다시 열지 못한다.
- Worker 수 0 또는 33을 barrier와 group configuration이 거부한다.
- Remote source/target metric의 terminal accounting이 보존된다.

테스트는 [`worker_actor_test.cpp`](../../tests/worker_actor_test.cpp)에 있다.

## 10. 한 문장으로 요약

```text
6A는 Actor message를 Worker 경계 너머로 전달하고,
6B는 그 message가 이동 중일 때 shutdown이 완료되었다고 오판하지 못하게 한다.
```

Publication epoch는 “새로운 event가 있었는가?”를 기록하고 conditional quiescence commit은 오래된 empty
관측이 종료 판정에 사용되는 것을 막는다. 이 둘을 ingress 차단과 Worker-only publisher 전제와 함께
적용해야 cross-worker Actor work를 잃지 않는 최소 shutdown correctness가 성립한다.
