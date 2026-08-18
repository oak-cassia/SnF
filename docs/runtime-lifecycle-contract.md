# Runtime Lifecycle 계약

> 범위: 서버 전체 drain 판정, 실패·취소 전파와 graceful shutdown 순서

## 1. Drain predicate

각 subsystem은 자신의 local predicate만 소유한다.

```text
ServerDrained =
    ActorRuntimeDrained
    ∧ NetworkRuntimeDrained
    ∧ PersistenceDrained
```

```text
ActorRuntimeDrained =
    external ingress closed and empty
    ∧ all Actor mailboxes empty
    ∧ ready queue empty
    ∧ no running or suspended task
    ∧ in-flight operation count == 0
    ∧ continuation queue empty
```

pending timer는 `ActorRuntimeDrained`의 항이 아니다. 승인된 작업이 아니라 아직 일어나지 않은
작업이므로 ingress close 시점에 폐기하고 선점된 outstanding reservation을 반납한다. drain 조건에
넣으면 스스로 재무장하는 주기적 tick이 predicate를 영원히 거짓으로 만든다.

```text
NetworkRuntimeDrained =
    listener stopped
    ∧ no active Session
    ∧ no pending lifecycle event
    ∧ outbound actions empty
    ∧ pending sends empty
    ∧ no active zone handoff
```

```text
PersistenceDrained =
    snapshot queue empty
    ∧ no pending Player snapshot
    ∧ no in-flight save
    ∧ no final save request
```

## 2. Runtime completion

`RuntimeCompletionCoordinator`가 required runtime의 drained/failed bit을 atomic 상태로 추적한다.
상태는 level-triggered이며 `eventfd`는 reactor가 상태를 다시 확인하게 하는 wake-up hint다. wake가
합쳐져도 drained/failed 사실은 사라지지 않는다.

현재 Logic Runtime만 coordinator identity를 가지며 network 종료는 reactor loop가 직접 판정한다.
outbound queue가 비었다는 사실은 Actor Runtime 완료를 대신하지 않는다.

## 3. Graceful shutdown

```text
listener와 새 gameplay ingress 차단
→ active Session의 ConnectionClosed 게시
→ lifecycle retry queue drain
→ Actor Runtime close
→ pending timer 폐기와 reservation 반납
→ Actor mailbox와 continuation drain
→ PlayerPersistenceService flush와 join
→ remaining outbound와 pending send drain
→ reactor 종료
```

입력 차단은 drain보다 먼저다. PlayerActor가 final snapshot을 제출한 뒤 persistence를 종료해야 하며,
저장은 outbound drain보다 먼저 끝낸다.

Actor가 outbound capacity를 기다리며 suspend될 수 있으므로 reactor는 Actor Runtime이 drained에
도달할 때까지 outbound를 계속 소비하고 reservation을 grant한다. reactor를 먼저 멈추면 Worker가
오지 않는 grant를 기다리는 종료 교착이 생긴다.

cross-zone handoff가 진행 중이면 route/token/completion reservation까지 정리돼야 network가
drained다. 단순히 Logic mailbox가 비었다는 이유로 reactor를 종료하지 않는다.

## 4. Cancel

- graceful close는 새 external command만 차단하고 승인된 continuation 경로를 유지한다.
- grace deadline 만료 또는 runtime failure는 cancel 경로로 전환한다.
- 외부 thread는 cancel request만 표시하고 Worker를 깨운다.
- owning Worker가 operation terminal claim, coroutine resume/destroy와 mailbox discard를 수행한다.
- completion과 cancel 중 하나만 terminal claim에 성공한다.
- 늦은 completion은 identity 검증에서 폐기한다.
- 한 subsystem의 failure가 다른 subsystem의 drain을 무한 대기로 만들지 않아야 한다.

## 5. 완료 조건

- 정상 종료 뒤 Actor, mailbox, in-flight, continuation과 persistence pending 수가 모두 0이다.
- outbound 포화 상태에서도 reactor가 grant를 계속해 종료가 완료된다.
- completion/cancel 동시 경합에서 terminal 결과는 하나다.
- transition 중 disconnect와 shutdown이 route나 reservation을 남기지 않는다.
- persistence background failure와 final save가 정의된 순서로 종결된다.
- Debug, ASan·UBSan과 TSan 반복 테스트를 통과한다.
