# SnF 개발 로드맵

> 이 문서가 구현 순서의 기준이다. 상세 구조는
> [서버 아키텍처 초안](./server-architecture-draft.md), Phase 4 안전 규약은
> [Coroutine Actor 계약](./coroutine-actor-contract.md)을 참조한다.

## 목표

```text
Network Runtime
→ Sharded Player Actor Runtime
→ Coroutine Actor
→ Auth/Persistence Vertical Slice
→ Minimal World/Battle
→ Shared Content
```

## 1. Network와 GameRuntime 분리 (완료)

- `ConnectionId{descriptor, generation}`으로 Session incarnation을 식별하고 stale 응답을 폐기한다.
- inbound/outbound bounded queue와 Reactor wake-up을 구현했다.
- Network thread는 게임 상태를 수정하지 않고 PING/PONG도 게임 Worker를 왕복한다.
- queue 포화, stale response, slow client와 graceful shutdown을 테스트한다.

## 2. Sharded ActorRuntime (완료)

- 기본 2 Worker와 Worker별 bounded outstanding capacity를 사용한다.
- 동일 Actor는 같은 Worker에서 FIFO·단일 실행되고 다른 shard는 병렬로 실행된다.
- close drain, cancel discard, 첫 Worker 예외 전파와 worker별 queue metric을 제공한다.

## 3. PlayerActor와 Mailbox (완료)

- `TcpServer → FrameIngress → ProtocolGateway(MessageDispatcher) → RoutedCommand → CommandRouter`
  경계를 구성했다.
- 미지원·잘못된 메시지는 Actor 생성과 mailbox 진입 전에 거부한다.
- `PlayerActor`만 `PlayerState`를 수정하며 typed `PlayerCommand`를 멤버 handler에서 처리한다.
- typed `PlayerResult`를 `ProtocolResponseMapper`가 outbound Frame으로 변환한다.
- Actor별 mailbox, ready token 중복 방지와 turn당 16개 fairness budget을 구현했다.

## 3.5. Coroutine 계약 준비와 경계 강화 (완료)

- `ConnectionId`를 net 계층으로 이동해 `net::Session`의 server 의존을 제거했다.
- 연결 generation 기반 키를 `ProvisionalActorId`로 명명했다. 이는 인증 전 routing key일 뿐이며
  `PlayerId`, DB key, 저장 key 또는 재접속 key가 아니다.
- `OutboundSink`가 bounded outbound queue와 backend wake-up을 감추므로 ActorRuntime은 raw queue와
  `eventfd`를 알지 않는다. 포화 시 blocking backpressure는 runtime stop token으로 중단 가능하며,
  한 Runtime의 취소가 공유 sink 자체를 취소하지 않는다.
- drained/failed는 outbound action과 분리했다. `RuntimeCompletionCoordinator`의 atomic 상태가
  authoritative하며 eventfd는 wake-up hint로만 사용한다.
- ASan·UBSan과 독립된 TSan preset을 추가했다.
- Phase 4의 continuation 전달, coroutine frame 수명, cancel과 drain 계약을 별도 문서로 확정했다.

## 3.7. effect 채널, protocol 분리, 연결 종료 전파 (완료)

- `PlayerResult`는 wire response 목록 대신 `PlayerEffect` 목록을 반환한다. 현재 effect는
  `SendResponse` 하나이며, handler가 정상 반환한 뒤에만 sink가 순서대로 적용한다.
- `ActorRuntime`은 `PlayerEffectSink`만 알고, protocol adapter인
  `ProtocolPlayerEffectSink`가 `ProtocolResponseMapper`와 `OutboundSink`를 조합한다.
  따라서 executor의 include graph에는 `Frame` 또는 outbound action이 없다.
- inbound command, outbound action, lifecycle, post result 헤더를 분리했다. `RuntimeId`는 모든
  `ActorRuntimeConfig` 생성 시 명시해야 한다.
- reactor가 관찰한 `ConnectionClosed{connection, cause}`는 기존 worker ingress FIFO의 command 뒤에
  게시된다. actor가 없는 close는 슬롯을 만들지 않으며, close를 소비한 owning worker만 actor를
  evict한다.
- lifecycle post가 포화되면 reactor 소유 pending deque가 회차당 최대 64건을 재시도한다. active
  session과 pending close는 하나의 bounded lifecycle slot 예산을 공유하며, 예산 소진 시 새 연결을
  Actor 생성 전에 거부한다. command overload 통계와 lifecycle 통계는 분리하며, shutdown에서는 닫힌
  ingress에 close를 재주입하지 않는다.

## 4. Coroutine Suspend와 Resume

- `PlayerActor` handler 반환형을 `ActorTask<PlayerResult>`로 바꾼다.
- 외부 command ingress와 Worker별 내부 continuation queue를 분리한다.
- async operation 시작 전에 in-flight와 terminal continuation capacity를 함께 예약한다.
- 외부 Worker는 coroutine을 직접 resume하지 않고 결과를 원래 Actor Worker로 게시한다.
- suspend 중 같은 Actor의 일반 command는 mailbox에서 기다리고 다른 Actor는 계속 진행한다.
- 취소와 late completion은 `ActorIncarnation + TaskId`로 구분한다.
- graceful drain은 외부 입력, mailbox, ready/running/suspended task와 continuation을 모두 관찰한다.
- blocking DB adapter는 coroutine scheduler 검증 후 bounded DB Worker Pool로 연결한다.

완료 기준:

- suspend 중 다른 Actor가 진행하고 원래 Actor는 terminal continuation 이후에만 재개된다.
- 성공·실패·취소 중 terminal continuation이 정확히 하나만 관측된다.
- completion/cancel, completion/shutdown, 즉시 completion, double completion, deadline 뒤 late
  completion, reservation 포화와 마지막 continuation/drain 경합이 테스트된다.
- resume과 coroutine frame 파괴가 owning Worker에서만 일어난다.
- suspend 전·후 예외 전파와 Debug, ASan·UBSan, TSan 검증이 통과한다.

## 5. 인증·영속성 Vertical Slice

하나의 멱등한 구매 흐름만 end-to-end로 완성한다.

```text
인증된 PlayerId
→ 비동기 player load
→ idempotency key가 있는 구매 요청
→ DB transaction으로 재화 차감과 상품 지급
→ 비동기 저장
→ disconnect/passivation
→ reconnect/복원
```

- `ConnectionId`, `ProvisionalActorId`, 영속 `PlayerId`를 서로 다른 타입과 용도로 유지한다.
- 동시 로그인, attach/detach/logout, Actor incarnation과 passivation 정책을 정의한다.
- at-least-once 재전달 가능성을 idempotency와 transaction으로 흡수해 effect를 한 번만 적용한다.
- DB Worker는 coroutine이나 Actor 포인터를 보유하지 않는다.

성장·업적·기간제 이벤트는 이 slice 이후 Player module로 추가한다. 랭킹과 시즌 정산은
Shared Content 또는 projection 단계로 미룬다.

## 6. 최소 World와 Battle

- 주입 가능한 Clock을 가진 FixedTickExecutor와 최소 Zone/CombatRoom을 구현한다.
- 이동, 기본 충돌, 전투 진입·종료와 멱등한 `BattleResult` 적용을 검증한다.
- `RouteCoordinator`가 `SessionRoute`와 `route_epoch`의 authoritative owner가 된다.
- route 변경 protocol이 이전 destination 정지, 새 destination 활성화, 새 route 공개 순서를
  보장한다. epoch은 원자성 구현이 아니라 stale destination 검출 수단이다.
- Player·World·Battle runtime을 `RuntimeCompletionCoordinator`의 required mask에 등록한다.
- tick 실행 시간, overrun과 command queue wait를 측정한다.

## 7. Shared Content와 Projection

- Party 또는 Matchmaking 하나를 공유 Actor의 대표 vertical slice로 구현한다.
- 랭킹 점수는 PlayerActor가 domain event를 발행하고 별도 projection이 집계한다.
- 시즌 정산은 재실행 가능한 job과 idempotent 결과 적용으로 구현한다.

## 선택적 인프라 트랙: io_uring Network Backend

io_uring은 콘텐츠 단계의 선행 조건이 아니다. epoll backend와 첫 영속성 slice로
`FrameIngress`, `OutboundSink`, lifecycle 계약을 검증한 뒤 진행한다.

- `NetworkRuntime` lifecycle 계약을 두 번째 backend가 실제로 필요로 하는 최소 범위에서 추출한다.
- `IoUringNetworkRuntime`은 동일한 `FrameEnvelope`를 제출하고 `OutboundAction`을 소비한다.
- operation/buffer lifetime, cancel과 stale completion을 검증한다.
- epoll과 동일한 기능·안정성·부하 테스트를 적용해 결과를 비교한다.

## 공통 원칙

- Network thread는 게임 상태를 수정하지 않는다.
- Handler는 물리 thread가 아니라 상태 소유 Actor 또는 simulation aggregate를 선택한다.
- 다른 Runtime의 mutable 객체나 coroutine handle을 직접 참조하지 않는다.
- 모든 비동기 queue와 in-flight operation에는 명시적인 상한이 있다.
- command별 순서, 멱등성, 포화와 전달 보장 정책을 정의한다.
- Service Layer, lock-free, microservice와 Actor 내부 병렬화는 실제 필요와 측정 전에는 도입하지
  않는다.
