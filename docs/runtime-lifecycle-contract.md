# Runtime Lifecycle 계약

> 상태: 현재 구현을 기술하고 목표 조건을 함께 표기하는 계약
> 범위: 서버 전체의 종료 판정, Runtime 사이의 drain 조합, 실패와 취소 전파, 종료 순서
>
> Actor runtime 내부 조건은 [Coroutine Actor 계약](./coroutine-actor-contract.md)이,
> 연결 하나의 네트워크 상태 소유권과 수명은 [ConnectionScope 계약](./connection-scope-contract.md)이,
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
`ConnectionScope`가 terminal 상태에 도달한 것을 뜻한다. 그 terminal 상태의 정의와 파괴 조건은
[ConnectionScope 계약](./connection-scope-contract.md) §3이 소유하며, 이 절의 재정의는 4.5 구현이
반영되는 시점에 갱신한다. 특히 `Retired`는 socket close와 같은 사건이 아니다. scope는
`ConnectionClosed`가 종결될 때까지 유지되므로 pending lifecycle 예산이 그 상태에 붙는다.

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

`ConnectionScope` 종료는 lifecycle coordinator가 추적한다. 별도 `RuntimeId::Net`을 추가할지는 단계 4.5
구현 시 결정하기로 미뤄둔 항목이었고, **추가하지 않기로 결정했다.** 4.5의 최소 Executor가 reactor
thread에서 실행되므로 network drain을 관측해야 하는 다른 스레드가 없고, 논리적인 drain predicate와
`RuntimeId` enum은 같은 결정이 아니므로 한 단계만 쓰이는 identity를 확정하지 않는다. 이 결정을 뒤집는
조건은 4.6에서 network drain을 coordinator가 관측해야 할 때다.

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

단계 4.1부터 이 순서에는 역방향 의존이 하나 있다. Actor가 outbound 용량을 기다리며 suspend될 수
있으므로 `ActorRuntimeDrained`가 outbound 소비에 의존한다. 따라서 reactor는 Logic Runtime이 drained에
도달할 때까지 outbound를 계속 소비하고 grant해야 한다. 목록의 "outbound queue drain"은 그 뒤에 남은
것을 비우는 단계이지 소비를 그때 시작한다는 뜻이 아니다.

reactor가 더 이상 grant할 수 없는 경로(reactor 실패, grace period 만료, 종료 중 queue 취소)에서는
outbound channel을 반드시 취소한다. 취소는 등록된 모든 waiter에게 취소된 예약을 terminal 결과로
전달하므로, 그것이 없으면 Worker가 오지 않는 grant를 기다리며 drain 판정이 영구히 성립하지 않는다.
`TcpServer`의 queue 취소와 `GameServer`의 runtime 취소가 모두 이 취소를 수행한다.

## 4. 실패와 취소

- graceful close는 외부 ingress만 닫고 이미 승인된 operation의 continuation 경로는 유지한다.
- cancel은 승인된 operation을 terminal cancellation으로 끝낸다. 결과를 버리는 것이 아니라 terminal
  결과의 한 종류다.
- Worker failure는 그 runtime을 failed로 표시하고 새 completion 적용을 차단한다. 첫 failure를 보존해
  상위로 전파한다.
- 한 Runtime의 취소가 공유 sink 자체를 취소하지 않는다. 단계 4.1부터 포화 대기는 blocking이 아니라
  Actor suspend이므로, 그 Runtime의 취소는 자신의 suspended task를 terminal cancel하고 frame을
  파괴하면서 예약 waiter까지 회수한다. 다른 Runtime은 같은 channel을 계속 사용한다.
- 종료 경로에서 닫힌 ingress에 lifecycle 이벤트를 재주입하지 않는다.

## 5. 검증

- 입력 차단, drain, 저장, outbound drain이 정의된 순서로 실행된다.
- outbound가 포화된 상태에서 graceful shutdown이 완료된다. reactor가 소비와 grant를 계속하므로 용량을
  기다리던 Actor가 진행하고, 그 뒤 Logic Runtime이 drained에 도달한다.
- reactor가 먼저 멈추는 실패 경로에서도 용량을 기다리던 Worker가 terminal 결과를 받아 join한다.
- `ServerDrained`가 참인 시점 이후에 어떤 subsystem도 새 작업을 시작하지 않는다.
- 한 subsystem의 failure가 다른 subsystem의 drain을 무한 대기로 만들지 않는다.
- cancel과 completion이 동시에 발생해도 terminal 결과는 정확히 하나다.
- 종료 중 재접속이나 stale `ConnectionId`로 향한 응답이 새 Session에 전달되지 않는다.
