# SnF 서버 아키텍처

> 범위: 현재 구현된 C++20, Linux `epoll` 기반 온라인 게임 서버의 상태 소유권과 실행 경계

## 1. 목표

SnF의 핵심 목표는 많은 서버 기능을 나열하는 것이 아니라 다음 질문에 답하는 것이다.

- 게임 상태를 어느 실행 단위가 수정하는가?
- 같은 Entity의 명령 순서는 어떻게 보장하는가?
- 느린 저장과 outbound가 게임 Worker를 어떻게 막지 않는가?
- queue가 가득 차거나 연결이 끊기면 승인된 작업은 어떻게 종결되는가?
- shutdown 완료를 어떤 상태로 판정하는가?

## 2. 전체 흐름

```text
Client
  ↓
TcpServer (epoll reactor)
  ↓ FrameEnvelope
ProtocolGateway / MessageDispatcher
  ↓ RoutedCommand
CommandRouter
  ↓ typed ActorSubmission
ActorRuntime
  ├── PlayerActor
  ├── ZoneActor
  └── PartyActor
  ↓ typed result/effect
OutboundChannel
  ↓
TcpServer
```

Network reactor는 Session, socket readiness와 route/coordinator 상태를 소유한다. Actor Worker는
domain mutable state만 소유한다. 어느 쪽도 상대 Runtime의 mutable 객체나 coroutine handle을
직접 보유하지 않는다.

## 3. 상태 소유권

| 소유자 | mutable state |
| --- | --- |
| Reactor | Session, connection generation, Player/Zone/Party route, handoff control state |
| PlayerActor | Player identity, 마지막 위치, currency, purchased item count, idempotency evidence |
| ZoneActor | Zone participant, position, route epoch, tick state |
| PartyActor | membership과 membership epoch |
| PlayerPersistenceService | pending/in-flight Player snapshot과 final save queue |
| Repository Worker | DB connection과 한 storage job의 로컬 상태 |

Player 내부 Session과 Economy는 코드 구성 단위이지 별도 Actor가 아니다. 자주 하나의 불변식으로
변경되는 상태는 같은 Actor에 둔다. 독립 mailbox와 backpressure가 필요하다는 측정이 있을 때만
Actor를 분리한다.

## 4. Actor 실행 모델

`ActorKey{ActorKind, EntityId}`의 hash로 owning Worker를 고정한다. Worker는 자신의 Actor table,
ready queue와 coroutine frame을 관리한다.

```text
post command
→ owning Worker ingress
→ Actor mailbox
→ ready queue
→ bounded turn
→ result or suspension
```

보장:

- 같은 Actor의 handler는 동시에 실행되지 않는다.
- Actor mailbox는 승인된 command의 FIFO를 유지한다.
- turn budget을 사용해 ready Actor 사이 cooperative fairness를 제공한다.
- suspend된 Actor의 다음 command는 기다리지만 같은 Worker의 다른 Actor는 진행한다.
- passivation은 active task, operation, continuation과 mailbox가 모두 없을 때만 가능하다.

한 hot Actor는 한 Worker보다 빠르게 처리될 수 없다. 이는 숨기는 문제가 아니라 Zone/Room 분할이나
배치 정책을 검토하게 하는 관측 대상이다.

## 5. Coroutine과 외부 작업

Actor handler는 lazy `ActorTask<Result>`다. 외부 작업을 시작하기 전에 in-flight slot과 terminal
continuation slot을 함께 예약한다.

```text
Actor Worker
  → reserve {ActorKey, Incarnation, TaskId}
  → submit immutable request + producer
  → suspend Actor

External executor
  → store value/error
  → publish terminal continuation

Owning Worker
  → validate identity
  → resume or discard stale completion
```

외부 executor는 Actor, ActorSlot 또는 coroutine handle을 받지 않는다. success, failure와 cancel 중
하나만 terminal claim에 성공하며, frame resume와 destruction은 owning Worker에서만 수행한다.
세부 경합 규칙은 [Coroutine Actor 계약](./coroutine-actor-contract.md)에 있다.

## 6. Backpressure

모든 cross-thread 자원은 bounded다.

| 경계 | 포화 정책 |
| --- | --- |
| Actor ingress/mailbox | command admission 거부 |
| in-flight continuation | 외부 작업 시작 전 거부 |
| Session pending bytes | 해당 connection 종료 |
| OutboundChannel | 즉시 예약 또는 해당 Actor만 suspend |
| persistence snapshot | dirty bit을 유지하고 다음 turn에서 재시도 |
| MySQL jobs | `Unavailable` completion |
| zone handoff completion | admission 시 slot 선예약 |

command terminal과 admission rejection은 다른 metric이다. 승인된 command는 response, typed failure,
connection close 또는 cancellation 중 하나로 종결하고 조용히 유실하지 않는다.

## 7. Network와 protocol

`TcpServer`는 level-triggered `epoll`, non-blocking accept/read/write와 `eventfd` wake-up을 사용한다.
Session은 partial frame과 pending send를 보존한다.

- frame: `[body_length:u32][type:u16][request_id:u32][payload]`
- 모든 정수는 big-endian이며 body는 최대 64 KiB다.
- decoder는 incomplete input과 invalid input을 구분한다.
- connection generation이 이전 연결의 늦은 outbound를 차단한다.
- protocol layer가 frame을 typed command로 바꾸므로 Actor는 wire format을 모른다.

## 8. Player persistence

Live gameplay state의 authority는 PlayerActor다. 구매 handler는 상품, 잔액, grant와 Actor 수명 범위
idempotency를 같은 turn에서 판정한다. 성공한 Economy 변경은 dirty snapshot으로 제출한다.

`PlayerPersistenceService`는 다음 규칙을 소유한다.

- snapshot queue 상한
- 같은 Player의 pending snapshot coalescing
- Player별 save 비중첩
- background 실패 retry
- logout/final save를 이전 background save 뒤에 직렬화
- shutdown final flush

Repository 계약은 `asyncLoad`와 `asyncSave`뿐이다. 기본 in-memory adapter는 결정적 테스트에,
MySQL adapter는 blocking C API를 전용 bounded Worker Pool에서 실행하는 실제 저장 경계에 사용한다.
DB Worker는 Actor 상태를 다시 판정하지 않고 immutable snapshot만 저장한다.

현재 NPC 구매는 저장 완료 전에 성공을 응답하는 deferred durability 정책이다. flush 전 process crash면
마지막 변경과 Actor 수명 범위 idempotency evidence가 사라질 수 있다. 이 위험을 허용할 수 없는
거래는 같은 경로에 옵션을 추가하지 않고 별도 요구사항과 authority를 정의해야 한다.

## 9. Shared Actor

### Zone

ZoneActor는 participant position과 tick을 직렬화한다. TimerService는 stale `TimerId`를 폐기하고
tick을 일반 Actor command로 게시한다. 빈 Zone은 route와 mailbox가 모두 정리된 후 passivate된다.

cross-zone 이동의 control state는 reactor의 `RouteCoordinator`가 소유한다. source leave, target
enter와 route publish를 단계적으로 진행하며 Worker completion은 bounded value channel로 돌아온다.
실패 보상과 disconnect/shutdown cleanup은 [Cross-Zone Handoff 계약](./cross-zone-handoff-contract.md)이
소유한다.

### Party

PartyActor가 membership을 직렬화하고 PartyCoordinator가 connection route와 epoch을 소유한다.
capacity 초과는 연결 오류가 아니라 typed 결과이며, 마지막 leave는 이미 승인된 mailbox tail을
버리지 않는 `PassivateIfIdle`을 사용한다.

## 10. Lifecycle

graceful shutdown 순서:

```text
new connection/input 차단
→ lifecycle command 게시
→ Actor mailbox와 continuation drain
→ PlayerPersistenceService final flush/join
→ outbound queue와 pending send drain
→ reactor 종료
```

outbound capacity를 기다리는 Actor가 있을 수 있으므로 reactor는 ActorRuntime drain 전까지 outbound를
계속 소비하고 reservation을 grant한다. 전체 predicate와 cancel 규칙은
[Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)에 있다.

## 11. 검증 전략

- 순수 Actor 상태 기계 단위 테스트
- deterministic hook을 사용한 completion/cancel 경합 테스트
- 최소 queue capacity에서 포화와 drain 테스트
- 실제 TCP frame, partial I/O와 reconnect 테스트
- fault injection과 shutdown ordering 테스트
- Release load client와 p50/p95/p99/max, high-water metric
- Debug, ASan·UBSan과 TSan preset

새 구조는 기존 수치가 병목이나 불변식 실패를 증명할 때만 추가한다. 구현되지 않은 backend와
executor 설계는 현재 아키텍처 문서의 범위가 아니다.
