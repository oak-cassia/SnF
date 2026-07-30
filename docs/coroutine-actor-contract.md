# Coroutine Actor 계약

> 상태: Phase 4 구현 전 확정 계약
> 범위: PlayerActor와 ZoneActor를 포함한 Logic ActorRuntime의 suspend, resume, cancel, drain과
> 외부 비동기 operation 수명

## 1. 신원

- `ConnectionId`는 하나의 network session incarnation만 식별한다.
- `ProvisionalActorId`는 인증 전 임시 routing key다.
- `PlayerId`는 인증 후 영속 domain identity이며 Phase 5에서 도입한다.
- 임시 ID는 DB key, 저장 key, idempotency scope 또는 재접속 복원 key로 사용하지 않는다.
- `ActorKey`는 `{ActorKind, EntityId}` 또는 동등한 discriminated key로 표현하는 scheduler의
  논리 Actor identity다. `ActorKind`는 인증 전 Player, 영속 Player, Zone 등의 ID namespace를
  구분하며 이 문서에서 별도의 `ActorIdentity` 타입은 두지 않는다.
- `ActorIncarnation`은 하나의 `ActorKey`가 활성화된 세대를 식별한다.
- async task는 `{ActorKey, ActorIncarnation, TaskId}`로 식별한다. 같은 Actor가 passivation 후
  다시 활성화되면 새로운 `ActorIncarnation`을 받는다.

## 2. Terminal continuation 보장

핵심 불변식은 다음과 같다.

> 시작이 승인된 async operation은 성공·실패·취소 중 정확히 하나의 terminal continuation을
> owning Worker에 반드시 전달한다.

- operation을 제출하기 전에 in-flight slot과 terminal continuation slot을 함께 예약한다.
- 예약 실패는 operation 시작 전 명시적인 busy/error 결과가 된다.
- 시작 후 completion enqueue가 `Full`로 거부되는 경로는 만들지 않는다.
- double completion은 두 번째 결과를 폐기하고 metric을 기록한다.
- immediate completion은 awaiter handshake를 거치며 coroutine을 외부 thread에서 inline resume하지
  않는다.

## 3. 소유권과 Worker affinity

- coroutine frame과 `ActorSlot`은 owning Actor Worker만 접근·resume·파괴한다.
- DB, timer 등 외부 executor는 raw `coroutine_handle`, `Actor*`, `ActorSlot*`을 보유하지 않는다.
- 외부 operation은 ref-counted operation state 또는 completion registry만 공유한다.
- 외부 결과는 `{ActorKey, ActorIncarnation, TaskId, Result}` value로 continuation ingress에
  게시한다.
- late completion은 incarnation과 task를 검증하고, 유효하지 않으면 operation state 정리만 수행한다.

## 4. 상태 전이

```text
Idle + mailbox command
→ Ready
→ Running
   ├── synchronous completion → Idle 또는 Ready
   └── co_await → Suspended
                    ├── terminal continuation → Running
                    └── cancel terminal event → Running → Idle
```

- `Suspended` Actor의 일반 command는 mailbox에 남는다.
- continuation은 일반 command보다 먼저 해당 Actor를 재개한다.
- network close는 같은 reactor가 만든 일반 command 뒤에 기존 ingress FIFO로 게시된다. 반대로
  continuation은 command 앞의 priority 규율을 사용하므로 두 입력을 하나의 queue 규칙으로 섞지 않는다.
- ready queue에는 Actor당 최대 하나의 token만 존재한다.
- suspended command는 완료 또는 취소 전까지 outstanding capacity를 점유한다.

## 5. 취소와 late completion

- graceful close는 외부 command ingress만 닫고 이미 승인된 operation의 continuation 경로는 유지한다.
- deadline 초과나 explicit cancel은 owning Worker에 terminal cancellation을 전달한다.
- blocking operation은 cancel 요청 뒤에도 끝나지 않을 수 있으므로 coroutine frame과 operation
  state를 분리한다.
- coroutine frame 파괴 후 도착한 외부 결과는 frame을 resume하지 않고 operation state와 외부
  resource만 정리한다.
- Worker failure는 모든 Actor task를 cancel 상태로 전환하고 새 completion 적용을 차단한다.

## 6. Drain과 passivation 조건

graceful drain은 다음 조건이 모두 참일 때만 완료된다.

```text
external ingress closed
&& external ingress queue empty
&& lifecycle close events in ingress empty
&& all actor mailboxes empty
&& ready queue empty
&& running task count == 0
&& suspended/in-flight task count == 0
&& continuation queue empty
&& reactor pending connection-close retry queue empty
```

Actor는 다음 조건에서만 passivation 가능하다.

```text
state == Idle
&& mailbox empty
&& no running task
&& no in-flight operation
&& no pending continuation
&& no lifecycle resource requiring retention
```

Phase 4에서는 passivation을 실행하지 않아도 되지만 이 조건을 metric/snapshot으로 관찰할 수 있어야
한다.

## 7. 필수 경합 테스트

- completion과 cancel 동시 발생
- completion과 graceful shutdown 동시 발생
- suspend 직전·직후 immediate completion
- 성공/실패/cancel double completion
- deadline 이후 late completion
- in-flight 또는 continuation reservation 포화
- suspend 전·후 handler/awaiter 예외
- resume과 coroutine frame 파괴 Worker affinity
- 마지막 continuation과 drain 완료 판정 경합

Debug와 ASan·UBSan 외에 별도 TSan 구성을 Phase 4 완료 gate로 사용한다.
