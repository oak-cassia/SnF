# Runtime Lifecycle 계약

> 상태: 현재 구현을 기술하고 목표 조건을 함께 표기하는 계약
> 범위: 서버 전체의 종료 판정, Runtime 사이의 drain 조합, 실패와 취소 전파, 종료 순서
>
> Actor runtime 내부 조건은 [Coroutine Actor 계약](./coroutine-actor-contract.md)이,
> 목표 구조와 불변식은 [서버 아키텍처 초안](./server-architecture-draft.md)이,
> 구현 순서는 [개발 로드맵](./development-roadmap.md)이 소유한다.

이 문서를 두는 이유는 종료 조건의 소유권을 한곳에 모으는 것이다. Actor 계약이 reactor의 pending
queue까지 함께 나열하면 ConnectionScope가 들어오는 순간 같은 목록이 세 문서로 갈라진다.

## 1. Drain predicate

각 subsystem은 자신의 local predicate만 소유하고, 전체 판정은 그 조합이다.

```text
ServerDrained =
    ActorRuntimeDrained
    ∧ NetworkRuntimeDrained
```

`ActorRuntimeDrained`의 구성 요소는 [Coroutine Actor 계약](./coroutine-actor-contract.md) §6이
소유한다. Phase 4.0 구현은 external ingress close/empty, 모든 Actor mailbox와 ready queue empty,
in-flight operation 0, continuation queue empty와 suspended/pending-resume 부재를 각각 명시적으로
검사한다. 이 판정은 Worker turn 사이에서 실행되므로 running task는 존재하지 않는다.

```text
NetworkRuntimeDrained =
    listener stopped
    ∧ no active connection
    ∧ outbound actions empty
    ∧ pending sends empty
    ∧ pending lifecycle events empty
```

`no active connection`의 현재 의미는 session map이 빈 상태다. 단계 4.5 이후에는 모든
`ConnectionScope`가 terminal 상태에 도달한 것을 뜻한다.

### 확장 규칙

소유자가 없는 predicate는 이 공식에 미리 넣지 않는다. DB, domain timer처럼 새 async subsystem을
도입하는 단계에서 그 subsystem의 local drained predicate를 정의하고 `ServerDrained`에 합성한다.

단계 4.6 이후에는 Connection, I/O continuation, Actor turn과 deadline이 한 pool에서 실행되므로 전체
판정을 다음과 같이 재조립한다.

```text
UnifiedRuntimeDrained =
    external ingress closed
    ∧ ready queue empty
    ∧ no running tasks
    ∧ no suspended operations
    ∧ no pending I/O completion
    ∧ no pending deadline
```

## 2. 현재 구현과의 대응

- `RuntimeCompletionCoordinator`가 required runtime mask에 대한 drained/failed를 authoritative
  atomic 상태로 추적한다.
- 상태는 level-triggered다. `eventfd` wake-up은 상태 재조회를 촉진하는 hint일 뿐이므로 wake가
  합쳐져도 완료 사실이 유실되지 않는다.
- runtime 완료 상태는 outbound queue와 분리한다. outbound action의 흐름이 완료 판정을 대신하지
  않는다.
- 현재 `RuntimeId`에는 `Logic` 하나만 있고 network 쪽 종료는 reactor loop 자신이 관측한다.
- Logic Runtime의 continuation queue는 hard cancel로 폐기하지 않는다. owning Worker가 suspended operation을
  terminal 처리해 in-flight가 0이 되고 모든 Worker가 join한 뒤 endpoint를 비활성화한다. 따라서
  completion claim과 publish 사이에 drain이나 runtime 파괴가 끼어들지 않는다.

`ConnectionScope` 종료는 lifecycle coordinator가 추적한다. 다만 별도 `RuntimeId::Net`을 추가할지는
단계 4.5 구현 시 결정한다. 논리적인 drain predicate와 `RuntimeId` enum은 같은 결정이 아니며, 한 단계만
쓰이는 identity를 미리 확정하지 않는다.

## 3. 종료 순서

graceful shutdown은 다음 순서를 따른다.

```text
새 연결 수락 중지
→ Session의 새 게임 command 수락 중지
→ Logic Actor Runtime에 종료 경계 전달
→ 모든 Actor mailbox와 continuation drain
→ coordinator가 required runtime의 drain 확인
→ 필요한 dirty state 저장 요청
→ DB queue drain 또는 timeout
→ outbound queue drain 또는 timeout
→ Reactor 종료
→ Logger flush
```

순서의 핵심은 입력 차단이 drain보다 먼저이고, 저장이 outbound drain보다 먼저라는 점이다. 종료 중에도
이미 파괴된 Session이나 Actor에 메시지를 보내지 않도록 Runtime 간 수명 순서를 지킨다.

## 4. 실패와 취소

- graceful close는 외부 ingress만 닫고 이미 승인된 operation의 continuation 경로는 유지한다.
- cancel은 승인된 operation을 terminal cancellation으로 끝낸다. 결과를 버리는 것이 아니라 terminal
  결과의 한 종류다.
- Worker failure는 그 runtime을 failed로 표시하고 새 completion 적용을 차단한다. 첫 failure를 보존해
  상위로 전파한다.
- 한 Runtime의 취소가 공유 sink 자체를 취소하지 않는다. 포화 대기를 중단하는 것은 그 Runtime의 stop
  token이며, 다른 Runtime은 같은 sink를 계속 사용한다.
- 종료 경로에서 닫힌 ingress에 lifecycle 이벤트를 재주입하지 않는다.

## 5. 검증

- 입력 차단, drain, 저장, outbound drain이 정의된 순서로 실행된다.
- `ServerDrained`가 참인 시점 이후에 어떤 subsystem도 새 작업을 시작하지 않는다.
- 한 subsystem의 failure가 다른 subsystem의 drain을 무한 대기로 만들지 않는다.
- cancel과 completion이 동시에 발생해도 terminal 결과는 정확히 하나다.
- 종료 중 재접속이나 stale `ConnectionId`로 향한 응답이 새 Session에 전달되지 않는다.
