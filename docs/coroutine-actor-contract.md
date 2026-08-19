# Coroutine Actor 계약

> 범위: Logic Actor Runtime의 suspend, resume, cancel과 외부 비동기 작업 수명

## 1. Identity와 소유권

- `ActorKey{ActorKind, EntityId}`는 논리 Actor를 식별한다.
- `ActorIncarnation`은 같은 Actor가 다시 활성화됐을 때 이전 수명과 구분한다.
- async task는 `{ActorKey, ActorIncarnation, TaskId}`로 식별한다.
- coroutine frame과 `ActorSlot`의 resume·destroy는 owning Worker만 수행한다.
- 외부 executor는 Actor, slot, coroutine handle을 보유하지 않는다. 결과와 identity value만
  continuation ingress에 게시한다.
- identity가 맞지 않는 late completion은 Actor를 건드리지 않고 폐기한다.

`ConnectionId`와 인증 전 provisional ID는 network session의 임시 신원이다. DB key와 재접속 복원에는
인증 후 `PlayerId`만 사용한다.

## 2. Terminal continuation 불변식

> 시작이 승인된 async operation은 성공·실패·취소 중 정확히 하나의 terminal 결과로 끝난다.

operation을 시작하기 전에 in-flight slot과 continuation slot을 함께 예약한다. 예약할 수 없으면 작업을
시작하지 않고 typed busy/error를 반환한다. 시작한 뒤 completion queue 포화로 결과를 잃는 경로는 없다.

```text
result 계산
→ Pending → Completed terminal claim
→ result 저장
→ 예약된 continuation 게시
→ owning Worker가 identity 확인 후 resume
```

- completion과 cancel 중 terminal claim 하나만 성공한다.
- `complete()`와 `fail()`은 claim 뒤 blocking, allocation 또는 throw를 하지 않는다.
- immediate completion도 외부 thread에서 coroutine을 inline resume하지 않는다.
- in-flight slot은 Worker가 continuation을 소비하거나 cancel claim을 완료한 뒤 해제한다.
- completion endpoint는 ref-counted이며 Runtime 종료 뒤 publish는 안전하게 거부된다.

## 3. Actor 상태 전이

```text
Idle + command
→ Ready
→ Running
   ├── result → Idle 또는 Ready
   └── co_await → Suspended
                    └── completion/cancel → Running → terminal
```

- 같은 Actor의 handler는 동시에 실행되지 않는다.
- suspend 중 일반 command는 mailbox FIFO에 남고 continuation이 먼저 Actor를 재개한다.
- ready queue에는 Actor당 token이 최대 하나다.
- 한 Actor는 한 시점에 외부 operation 하나만 기다린다. 한 command 안의 순차 await는 허용한다.
- 외부 waiter 등록은 coroutine frame의 guard가 소유해 cancel과 frame 파괴 뒤 registry에 남지 않는다.
- queue wait와 outstanding metric은 command당 한 번만 기록하고 resume 때 중복 계산하지 않는다.

network close는 reactor의 session lifecycle queue를 통해 기존 ingress 순서 뒤에 게시한다. 전체 network와
Actor 종료 순서는 [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)이 소유한다.

## 4. Cancel과 failure

- graceful close는 새 command만 차단하고 승인된 continuation 경로는 유지한다.
- deadline 또는 runtime failure는 cancel request를 표시하고 Worker를 깨운다.
- owning Worker가 `Pending → Cancelled` claim, resume와 frame 파괴를 수행한다.
- 이미 완료된 작업은 cancel이 재개하지 않고 기존 completion 게시를 기다린다.
- frame 파괴 뒤 도착한 결과는 operation state만 정리한다.
- Worker failure는 해당 Worker의 suspended task를 terminal cancel하고 새 completion 적용을 차단한다.

## 5. Scheduling과 Timer
 
- Actor는 `ActorContext::trySchedule(delay, submission)`으로 미래 시점의 자기 mailbox 알람을 예약할 수 있다.
- 예약 시점에 `reserveOutstanding()`으로 mailbox 자리를 선점하므로, 만료 시 발화 실패나 drop이 발생하지 않는다.
- 알람 힙은 Worker별로 독립적으로 관리되며, Worker는 가장 이른 알람 시점까지만 대기(`waitUntil`)한다.
- 만료된 알람은 Worker 스레드 안에서 actor mailbox로 직접 push되어 `Ready` 전이를 일으킨다.
- Actor eviction 또는 worker shutdown 시 pending timer는 즉시 discard되며 선점된 outstanding reservation도 안전하게 반환된다.

Actor 간 메시지도 같은 자리에서 다룬다.

- Actor는 `ActorContext::tryTell(target, payload)`으로 다른 Actor의 mailbox에 명령을 보낼 수 있다.
- `tryTell`은 awaitable이 아니다. **Actor가 다른 Actor의 응답을 기다리는 것은 런타임 계약으로 금지한다.**
  불가능해서가 아니라, 순환 대기가 생겼을 때 이를 탐지하거나 해소하는 장치가 현재 런타임에 없기 때문이다.
  순환이 생기면 `Suspended` task가 영구히 남아 아래 drain predicate가 영원히 거짓이 된다.
- submission은 대상 kind에 등록된 Binding이 조립한다. 송신자는 대상의 command 타입을 알지 않는다.
- tell은 기존 `tryPost` 경로를 그대로 쓰므로 ingress 상한, 포화 정책과 FIFO 보장이 송신자에 따라 갈리지 않는다.
- FIFO는 한 송신자가 한 turn에 보낸 tell 사이에서만 보장된다.
- `makeTell`은 모든 Worker에서 동시에 호출되므로 읽기 전용 변환이어야 한다.

상세는 [Actor 간 메시지와 게임 시간 결정](./actor-messaging-and-game-time.md)에 있다.

## 6. Drain과 passivation

```text
ActorRuntimeDrained =
    external ingress closed and empty
    && all mailboxes empty
    && ready queue empty
    && running task count == 0
    && suspended/in-flight task count == 0
    && continuation queue empty
```

Actor passivation은 `Idle`, mailbox empty, task·operation·continuation 없음과 domain resource 없음이 모두
참일 때만 가능하다. 일반 비활성화는 `PassivateIfIdle`을 사용한다. mailbox tail을 버릴 수 있는 `Evict`는
lifecycle fence에만 사용한다.

## 7. 검증 조건

- completion과 cancel/shutdown 동시 경합
- suspend 직전·직후 immediate completion
- success/failure/cancel 중복 호출
- reservation 포화와 late completion
- handler/awaiter 예외
- resume와 frame destruction의 Worker affinity
- 마지막 continuation과 drain 판정 경합
- Debug, ASan·UBSan과 TSan 통과
