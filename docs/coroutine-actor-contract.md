# Coroutine Actor 계약

> 상태: Phase 4.1 구현 반영. 첫 production awaiter는 outbound 용량 예약이다.
> 범위: PlayerActor와 ZoneActor를 포함한 Logic ActorRuntime의 suspend, resume, cancel,
> `ActorRuntimeDrained`와 외부 비동기 operation 수명
>
> 이 문서는 Actor runtime 내부 조건만 소유한다. 서버 전체 종료 판정과 Runtime 사이의 조합은
> [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)이, 연결 하나의 네트워크 상태 소유권과
> 수명은 [ConnectionScope 계약](./connection-scope-contract.md)이 소유한다.

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

terminal claim과 continuation publish 사이는 다음 순서를 지킨다.

```text
result 계산
→ complete() 진입
→ Pending → Completed CAS로 terminal claim
→ result 저장
→ 예약된 continuation endpoint에 publish
→ complete() 반환
```

- `complete()`와 `fail()`은 `noexcept`다. claim 이후에는 blocking, allocation, throw 또는 중간 반환을
  허용하지 않는다. 예약된 queue slot에 enqueue하기 위해 continuation queue mutex 하나만 잠근다.
- `Completed`를 먼저 관측한 cancel 경로는 직접 resume하지 않고 claimer가 반드시 게시할 continuation을
  기다린다.
- continuation queue는 cancel하지 않는다. Worker가 모든 in-flight operation을 terminal 처리하고 join한
  뒤에만 endpoint를 비활성화하고 queue storage의 수명을 끝낸다.
- in-flight reservation은 Worker가 continuation을 소비하거나 `Pending → Cancelled` claim에 성공한
  시점에만 해제한다. 따라서 drain 판정이 claim과 publish 사이 operation을 앞지를 수 없다.
- claimer thread가 claim 뒤 publish 전에 영구 정지하면 Worker도 진행하지 못하는 잔여 위험을 받아들인다.
  이 좁은 구간의 non-blocking/no-allocation 전제가 깨지면 `Completing → Published` 2단 상태로 확장한다.

## 3. 소유권과 Worker affinity

- coroutine frame과 `ActorSlot`은 owning Actor Worker만 접근·resume·파괴한다.
- DB, timer 등 외부 executor는 raw `coroutine_handle`, `Actor*`, `ActorSlot*`을 보유하지 않는다.
- 외부 operation은 ref-counted operation state 또는 completion registry만 공유한다.
- 외부 executor는 ref-counted operation state에 `Result`를 기록하고 continuation ingress에는
  `{ActorKey, ActorIncarnation, TaskId}` identity value만 게시한다. 둘 다 Actor나 coroutine handle을
  포함하지 않는다.
- late completion은 incarnation과 task를 검증하고, 유효하지 않으면 operation state 정리만 수행한다.
- completion endpoint는 ref-counted다. 외부 operation이 runtime보다 오래 살아도 비활성 endpoint가
  publish를 조용히 거부하며 runtime 또는 coroutine frame을 역참조하지 않는다.

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
  그 게시가 승인된 command 뒤에 온다는 보장은
  [ConnectionScope 계약](./connection-scope-contract.md) §3이 소유한다.
- ready queue에는 Actor당 최대 하나의 token만 존재한다.
- suspended command는 완료 또는 취소 전까지 outstanding capacity를 점유한다.
- Actor 하나는 동시에 하나의 operation만 await한다. 한 command의 순차적인 여러 await는 허용하지만
  `when_all` 같은 다중 outstanding operation은 Phase 4.0 범위가 아니다. Phase 4.1의 binding이 이
  순차 허용에 의존한다. handler task와 예약 task는 서로 다른 task이며, continuation을 소비할 때
  slot의 operation 등록이 해제되므로 같은 command 안에서 두 번째 await가 성립한다.
- 외부 자원의 waiter registry에 등록하는 awaiter는 등록 handle을 coroutine frame 안의 guard로
  보유한다. suspend된 frame을 파괴할 때도 그 guard가 실행되므로, per-operation cancel과 runtime
  전체 cancel 모두 registry에 waiter를 남기지 않는다. Phase 4.1의 outbound 예약이 이 방식을 쓴다.
- command queue wait는 최초 dispatch에서 한 번만 기록하고 resume에서는 다시 기록하지 않는다.
  outstanding은 terminal 성공·실패·취소에서 정확히 한 번 해제하며, processed는 최종 성공에서만
  증가한다. resume은 새 Actor turn이지만 같은 command의 turn budget을 다시 소비하지 않는다.

## 5. 취소와 late completion

- graceful close는 외부 command ingress만 닫고 이미 승인된 operation의 continuation 경로는 유지한다.
- deadline 초과나 explicit cancel은 owning Worker에 terminal cancellation을 전달한다.
- 외부 cancel은 atomic request flag만 설정하고 Worker를 깨운다. `Pending → Cancelled` claim,
  coroutine resume와 frame 파괴는 owning Worker만 수행한다.
- blocking operation은 cancel 요청 뒤에도 끝나지 않을 수 있으므로 coroutine frame과 operation
  state를 분리한다.
- coroutine frame 파괴 후 도착한 외부 결과는 frame을 resume하지 않고 operation state와 외부
  resource만 정리한다.
- Worker failure는 모든 Actor task를 cancel 상태로 전환하고 새 completion 적용을 차단한다.
  실패한 Worker는 pump로 돌아오지 않으므로 자신의 suspended task를 failure 경로에서 직접 terminal
  cancel하고 frame을 파괴한다.

## 6. Drain과 passivation 조건

`ActorRuntimeDrained`는 다음 조건이 모두 참일 때만 참이 된다.

```text
ActorRuntimeDrained =
    external ingress closed
    && external ingress queue empty
    && lifecycle close events in ingress empty
    && all actor mailboxes empty
    && ready queue empty
    && running task count == 0
    && suspended/in-flight task count == 0
    && continuation queue empty
```

network 쪽 조건(listener 정지, pending lifecycle 재시도, outbound와 pending send)은 이 목록에 넣지
않는다. `NetworkRuntimeDrained`와 두 predicate의 조합은
[Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)이 소유한다.

Actor는 다음 조건에서만 passivation 가능하다.

```text
state == Idle
&& mailbox empty
&& no running task
&& no in-flight operation
&& no pending continuation
&& no lifecycle resource requiring retention
```

Phase 4.0은 passivation을 실행하지 않는다. runtime이 판단할 수 있는 scheduler 조건만
`scheduler_passivatable_actor_count`로 관찰하며, lifecycle resource retention까지 판정하지 않는다.

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

Phase 4.0의 synthetic async binding이 위 경합을 검증한다. production `PlayerActor`는
`ActorTask<PlayerResult>`를 반환하지만 await할 외부 operation이 없어 첫 resume에서 동기 완료한다.
첫 production suspension point는 Phase 4.1의 outbound 용량 예약이며, handler가 아니라 binding이
그것을 await한다.

Phase 4.1은 포화가 아니면 operation을 시작하지 않는다. 예약이 즉시 성공하면 in-flight slot,
continuation, suspend가 모두 없으므로, 이 계약의 경합 규약은 포화 상태에서만 적용된다. 예약 대기가
Worker의 in-flight 예산을 소비하는 것도 포화 상태에서만이며, Phase 5의 DB await와 같은 예산을
나눠 쓴다.
