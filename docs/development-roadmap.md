# SnF 개발 로드맵

> 현재 우선순위: **Unified Worker Runtime 전환**
> 목표 구조: [Unified Worker Runtime 아키텍처](./unified-worker-runtime.md)
> 원칙: wire/gameplay 의미와 기존 검증 가능한 동작을 보존하면서 실행 경계를 단계적으로 교체한다.

## 1. 전환 전 기준

현재 코드는 별도 network Reactor, `ActorRuntime`, `ActorBinding`, shared `OutboundChannel`,
`PlayerPersistenceService`와 blocking MySQL worker를 사용한다. 루트 README와 2026-08-23 Room 부하
리포트는 이 구현의 설명과 baseline이다.

목표 구조는 다음을 제거하거나 흡수한다.

| 현행 구조 | 목표 |
| --- | --- |
| 별도 Reactor와 Actor Worker | connection, Actor와 native DB I/O를 소유하는 통합 Worker |
| ActorRuntime registry/scheduler | Worker-local ActorTable + ReadyActorQueue |
| ActorBinding | typed dispatch + 순수 `toEffects` overload |
| shared OutboundChannel | connection owner의 bounded write buffer |
| 별도 continuation/completion bookkeeping | `ActorSlot.blocked` 하나 |
| PlayerPersistenceService + blocking MySQL worker | Worker-local native async DbClient; 동기 API는 선택 adapter |
| reactor-owned workflow table/service | 자연스러운 Actor의 explicit state, 필요 시 Coordinator Actor |

전환 도중 두 런타임 경로가 같은 command나 outcome을 이중 실행하지 않게 한다. 한 vertical slice는 한
경로에서만 authority를 가진다.

## 2. 보존할 동작

- binary frame, request ID와 server-push 의미
- connection generation을 통한 stale session 차단
- Player, Zone, Room별 단일 mutable-state owner와 FIFO 처리
- Room 입장·실패 보상·Zone 복귀의 gameplay 결과
- cross-zone handoff의 epoch와 stale completion 방어
- Player snapshot의 domain authority와 현재 명시된 durability 한계
- slow consumer가 다른 connection과 Room 진행을 멈추지 않는 정책
- Actor와 resource admission의 bounded/backpressure 의미
- 현재 Debug, sanitizer, TCP, MySQL과 load test의 재현 가능한 시나리오

구현 타입 이름, queue topology와 thread 배치는 보존 대상이 아니다.

## 3. 구현 순서

### 0단계 — 문서 기준 정리

- [x] Unified Worker Runtime을 유일한 target runtime 문서로 등록한다.
- [x] 기존 ActorRuntime/OutcomeHandler 중심 설계 문서를 제거한다.
- [x] 루트 README와 부하 리포트를 현행 구현/baseline으로 명시한다.
- [x] domain handoff/persistence 계약에서 legacy topology의 권한을 내린다.

종료 조건: 새 구현 결정이 어느 문서를 따라야 하는지 한 곳에서 찾을 수 있다.

### 1단계 — Core identity

- [x] `ActorKey`, `ActivationRef`, `ConnectionRef`, `AwaitKey`를 확정한다.
- [x] Actor incarnation, connection generation과 operation ID의 생성·비교 규칙을 고정한다.
- [x] old incarnation/generation/operation event가 state를 변경하지 않는 connection/registration test를 만든다.

종료 조건: 모든 delayed event의 stale 여부를 pointer 없이 value identity로 판정한다.

### 2단계 — Worker skeleton

- [x] Worker-local Poller, WorkerInbox와 TimerQueue를 만든다.
- [x] poll, inbox, timer, Actor, write phase와 count/byte/time budget을 구현한다.
- [x] `stop_requested + wakeup`과 empty-loop shutdown을 검증한다.
- [x] owner-thread assertion을 추가한다. watchdog stall metric은 Actor/DB phase에서 보강한다.

종료 조건: runnable phase가 서로 starvation시키지 않고 Worker가 잠재적 blocking 호출을 하지 않는다.

### 3단계 — Connection path

- [x] accept 시 connection owner를 정하고 `ConnectionRef`에 generation과 owner를 저장한다.
- [x] read/decode/request translation 경계를 owner Worker의 `RequestSink`로 구현한다.
- [x] local send는 write buffer에 직접 append하고 remote send/close는 WorkerInbox event로 전달한다.
- [x] read/write/frame/table 상한과 slow-consumer close 경로를 검증한다.

종료 조건: shared OutboundChannel 없이 partial I/O와 기존 protocol 통합 테스트를 통과한다.

### 4단계 — Actor path

- [x] Worker-local ActorTable, bounded mailbox와 ReadyActorQueue를 만든다.
- [x] `Idle/Queued/Running/Stopping` 상태 전이와 synchronous immediate construction을 구현한다.
- [x] local delivery primitive(`tryDeliverLocal`)와 mailbox 기반 non-reentrant FIFO command turn(`CompletedTurn`)을 실행한다.
- [x] `SendFrame`, `CloseConnection`, `TellActor`, `StopActor` 네 concrete effect의 ordered batch 적용을 검증한다.
- [x] Actor quiescence와 connection drain을 단일 absolute deadline으로 통합한 graceful shutdown을 구현한다.

종료 조건: single-worker 결정성, non-reentrancy, duplicate-ready 방지와 concrete effect ordered application을 검증한다.

### 5단계 — BlockedTask와 async activation

- [ ] `Loading`, `Suspended` 상태와 nullable ActorSlot의 실제 비동기 activation을 구현한다.
- [ ] `ActivationLoad`와 `SuspendedDbCommand`를 `ActorSlot.blocked`에 저장한다.
- [ ] `SuspendedTurn`, continuation, concurrent Loading cap을 추가한다.
- [ ] timeout 후 late completion과 completion 후 stale timeout을 모두 no-op으로 만든다.

종료 조건: Actor가 기다리는 상태를 `ActorSlot.blocked` 한 곳에서만 찾을 수 있고 비동기 continuation이 시작한 Worker에서 재개된다.

### 6단계 — Cross-worker event와 Public Tell

- [ ] 최종 public `Worker::tell()`을 추가하고 `WrongOwner` 분기를 remote delivery로 전환한다.
- [ ] remote actor message, connection send/close와 실제 필요한 application completion을 concrete
  `WorkerEvent`로 정의한다.
- [ ] MPSC queue를 count/byte bounded로 만들고 enqueue 실패를 caller에게 반환한다.
- [ ] WorkerGroup producer quiescence와 shutdown barrier를 구현한다.

종료 조건: 다른 Worker가 owner object pointer를 보관하거나 queue 자리를 기다리지 않는다.

### 7단계 — Result와 Effect Adapter

- [ ] Player/Zone/Room domain adapter에서 typed result를 `toEffects(context, result)` overload로 변환한다.
- [ ] `ScheduleTimerEffect` 등 추가 Effect별 failure semantics와 stop-batch 정책을 적용한다.
- [ ] 실제 fan-out 측정에 따라 `EffectBatch` cap을 재결정한다.
- [ ] `RequestSink`에서 실제 game request translation과 Actor ingress를 연결한다.
- [ ] Domain 코드에서 Worker, ActorTable, ConnectionTable과 send/tell 직접 호출을 제거한다.

종료 조건: Worker가 typed domain result 의미를 알지 않고 별도 OutcomeHandler hierarchy가 없다.

### 8단계 — Native async DB

- [ ] production DB driver의 acquire, DNS, connect, TLS/auth, query, fetch, cancel과 reconnect가 모두
  non-blocking인지 conformance test로 증명한다.
- [ ] Worker-local DbClient, DbInFlight와 `completeDb()`를 구현한다.
- [ ] `Rejected/CompletedInline/Pending` submit 계약과 DB-local capacity를 적용한다.
- [ ] Player activation/load/save 의미를 새 DB 경로에 맞게 확정한다.

종료 조건: DB progress는 Worker poller에서 진행하고 DbClient는 Actor pointer나 coroutine을 소유하지 않는다.

### 9단계 — 선택 adapter

- [ ] native async로 교체하지 못한 동기 API 목록이 있을 때만 BlockingAdapterExecutor를 만든다.
- [ ] immutable job과 `AwaitKey`만 넘기고 completion reservation, saturation과 cancel을 검증한다.
- [ ] CPU-heavy 작업이 확인될 때만 별도 CpuExecutor를 검토한다.

종료 조건: adapter가 없는 build/configuration에서도 core runtime이 완전하다.

### 10단계 — Application workflow 이전

- [ ] Room entry/return과 cross-zone transition의 natural owner를 결정한다.
- [ ] natural domain owner가 있으면 explicit Actor state + correlation ID로 구현한다.
- [ ] 독립 lifecycle이 실제 필요한 흐름만 Coordinator Actor로 만든다.
- [ ] Actor-to-Actor mailbox 응답을 suspended coroutine이 기다리게 하지 않는다.
- [ ] disconnect, timeout, compensation과 shutdown terminal을 기존 contract와 대조한다.

종료 조건: 별도 WorkflowTable 없이 모든 accepted transition이 성공, 명시적 실패, close 또는 cancel로 끝난다.

### 11단계 — 운영 검증과 legacy 제거

- [ ] Worker/Actor/Inbox/DB/Connection/Timer/Completion metric을 추가한다.
- [ ] client I/O, hot Actor, 느린 DB와 slow consumer를 동시에 부하한다.
- [ ] Debug, ASan·UBSan, TSan, TCP/MySQL integration과 shutdown race를 통과한다.
- [ ] production server의 connection/game request 경로를 신규 Worker로 100% 전환한다.
- [ ] 전환 뒤 ActorRuntime, Binding, shared Outbound와 legacy completion 코드를 제거한다.
- [ ] 루트 README를 목표 구조의 실제 코드 링크와 새 측정값으로 갱신한다.

종료 조건: target architecture의 품질 게이트를 모두 통과하고 README가 더 이상 legacy 배너를 필요로 하지
않는다.

## 4. 품질 게이트

| 게이트 | 통과 조건 |
| --- | --- |
| Thread ownership | TSAN과 owner assertion에서 cross-thread mutable access 0건 |
| No Worker blocking | slow backend 주입 중 watchdog stall 0건 |
| Memory bound | inbox, mailbox, ActorTable, Loading, timer, DB queue와 buffers가 설정 상한을 넘지 않음 |
| Single await state | continuation/deadline을 `ActorSlot.blocked` 밖에 중복 저장하지 않음 |
| Stale safety | stale event가 state mutation을 만들지 않음 |
| Fairness | 지속 부하에서도 모든 event-loop phase가 반복 실행됨 |
| Shutdown | hard deadline 안에 coroutine/resource leak 없이 종료 |
| Behavior parity | 보존 대상으로 정한 protocol, gameplay와 failure outcome의 회귀 없음 |

## 5. 전환 중 열지 않는 작업

- Actor live migration과 dynamic Worker scaling
- reentrant Actor
- generic async backend interface와 범용 completion dispatcher
- 범용 workflow engine, durable saga/outbox
- idle passivation 선행 구현
- 별도 TimerService와 shared OutboundQueue 재도입
- Projectile 전용 Worker 또는 물리 partition

이 항목은 실제 부하, 두 번째 backend 또는 process-restart durability 요구가 생겼을 때 별도 설계한다.
