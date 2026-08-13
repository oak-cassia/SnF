# SnF 개발 로드맵

> 이 문서가 구현 순서와 완료 여부의 단일 기준이다. 실행 모델 전환의 근거와 단계 개요는
> [UnifiedRuntime 전환 개요](../study/10-unified-runtime-overview.md), 목표 구조와 불변식은
> [서버 아키텍처 초안](./server-architecture-draft.md), Actor coroutine 안전 규약은
> [Coroutine Actor 계약](./coroutine-actor-contract.md), 전체 종료 판정은
> [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)을 참조한다.

## 목표

```text
Network Runtime
→ Sharded Player Actor Runtime
→ Actor-Bound Logic Runtime 일반화
→ Backpressure 계약과 계측
→ Actor Coroutine
→ Async Outbound Reservation
→ Network Correctness
→ Playable Player Session Vertical Slice
→ Transactional Gameplay Slice
→ Shared Content
→ 측정 후 Runtime 최적화 (선택)
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
- inbound command, outbound action, lifecycle, post result 헤더를 분리했다. Phase 3.8 이후
  completion identity는 단일 `RuntimeId::Logic`이며 `ActorRuntimeConfig`에 별도로 지정하지 않는다.
- reactor가 관찰한 `ConnectionClosed{connection, cause}`는 기존 worker ingress FIFO의 command 뒤에
  게시된다. actor가 없는 close는 슬롯을 만들지 않으며, close를 소비한 owning worker만 actor를
  evict한다.
- lifecycle post가 포화되면 reactor 소유 pending deque가 회차당 최대 64건을 재시도한다. active
  session과 pending close는 하나의 bounded lifecycle slot 예산을 공유하며, 예산 소진 시 새 연결을
  Actor 생성 전에 거부한다. command overload 통계와 lifecycle 통계는 분리한다. graceful shutdown은
  활성 session의 close를 이 retry 경로로 모두 승인한 뒤 ingress를 닫는다.

## 3.8. Actor-Bound Logic Runtime 일반화 (완료)

Coroutine state machine을 추가하기 전에 `ActorRuntime`의 scheduler mechanism과 Player
domain policy를 분리했다. `snf::runtime::ActorRuntime`은 binding registry와 generic scheduler만
소유하고, Player typed command와 effect 적용은 `PlayerActorBinding`이 소유한다.

- scheduler용 논리 신원으로 `ActorKey{ActorKind, EntityId}`를 도입한다. `ActorKind`는 최소한
  인증 전 `ProvisionalPlayer`, 영속 `Player`, `Zone`의 ID namespace를 구분하거나 동등한
  discriminated key를 사용한다. `ProvisionalActorId`를 영속 `PlayerId`와 같은 namespace로
  평탄화하지 않는다.
- Worker 선택을 `hash(ActorKey) % worker_count`로 통일한다. `ActorKind`는 key의 동등성과
  hash 모두에 포함된다.
- scheduler 계층은 shard ingress, Actor별 FIFO mailbox, ready queue, turn budget, bounded
  outstanding capacity와 Worker lifecycle만 소유한다. Actor 활성화, typed command dispatch,
  handler result와 effect 적용은 Actor binding 계층이 소유한다.
- `PlayerCommandRoute`, 향후 `ZoneCommandRoute`처럼 target과 command가 결합된 typed route
  경계를 유지한다.
  임의의 `ActorKey`-message 조합을 허용하는 public `AnyMessage` ingress는 만들지 않는다.
- `ConnectionClosed`는 모든 Actor의 공통 domain event로 올리지 않는다. Player binding이 인증 전
  Actor의 ordered lifecycle control로 변환하되, 기존 command 뒤에서 소비되고 Actor가 없으면
  slot을 생성하지 않는 현재 의미를 유지한다.
- 하나의 Worker Pool이 여러 Actor 종류를 실행하므로 runtime completion identity를
  `RuntimeId::Logic`으로 정리한다. drain/failure는 Actor 종류별이 아니라 Logic Runtime
  전체의 terminal state를 의미한다.
- 기존 PING/PONG, FIFO, fairness, queue capacity, lifecycle retry, drain/cancel/failure와
  blocking outbound backpressure 의미를 변경하지 않는다. Timer Scheduler, 실제
  `ZoneActor`, coroutine은 이 단계에 구현하지 않는다.

완료 기준:

- 같은 `ActorKey`는 활성화된 동안 항상 같은 Worker에서만 실행된다.
- `{Player, 1}`과 `{Zone, 1}`, `{ProvisionalPlayer, 1}`과 `{Player, 1}`은 각각 별도의
  ActorSlot과 mailbox를 갖는다. hash modulo 결과가 같아 같은 Worker에 배치되는 것은
  허용한다.
- production `ZoneActor` 대신 synthetic 두 번째 Actor 종류로 서로 다른 종류의 Actor가
  하나의 Logic Worker Pool, capacity와 fairness 규칙을 공유함을 검증한다.
- scheduler 계층의 public header와 translation unit이 `PlayerActor`, `PlayerCommand`,
  `PlayerResult`, `PlayerEffectSink`를 직접 참조하지 않는다.
- 기존 FIFO, exactly-once handling, turn yield, Worker 병렬성, queue 포화, close eviction,
  lifecycle retry, drain/cancel/failure 테스트가 통과하고 cross-kind 회귀 테스트가 추가된다.

## 3.9. Backpressure 계약과 계측 (완료)

포화 정책의 현재 동작과 목표 동작을 문서에 고정하고 baseline metric을 확보했다. 이 단계의 구현
산출물은 metric뿐이며 포화 동작 자체는 바꾸지 않았다.

현재 동작:

- inbound: `FrameIngress`가 `FramePostResult::Full`을 반환하면 reactor가 `actor_queue_overflows`를
  올리고 해당 연결을 `ConnectionCloseCause::Overflow`로 종료한다. frame을 조용히 버리지는 않는다.
- outbound: Logic Worker가 `BoundedQueue::push`에서 stop token으로만 중단 가능한 blocking 대기를 하며,
  그 queue를 비우는 주체는 reactor 하나다. (이 절은 3.9 시점의 동작이며 4.1이 이 대기를 제거했다.)
- lifecycle: `ConnectionClosed` post가 `Full`이면 reactor 소유 pending deque가 회차당 제한된 건수를
  재시도한다.

목표 동작:

- inbound: 연결별 in-flight credit이 소진되면 socket 읽기를 중지하고, command terminal 시점에 credit을
  반환해 읽기를 재개한다. admission 실패와 악성 과부하는 여전히 명시적 정책으로 종료한다.
- outbound: Logic Worker는 blocking 대기 대신 reservation을 await한다. (4.1에서 구현했다.)

계약으로 확정할 정의:

- **in-flight credit**: 하나의 연결이 아직 terminal에 도달하지 않은 상태로 ingress에 올릴 수 있는
  command 수의 상한이다. 연결 수명에 속하며 Actor 활성화와는 별개다. 실제로 느린
  command가 등장하는 Playable Session slice에서 연결 간 격리 필요를 측정한 뒤 현재
  reactor에 적용한다.
- **command terminal**: handler가 반환한 시점이 아니라 필요한 effect가 적용되고 성공, 실패 또는 취소
  중 하나의 최종 결과가 확정된 시점이다. 이때 credit 점유가 끝난다. 응답 effect가 있는 command와 없는
  command에 같은 정의를 적용한다. 신호의 생산 경로는 4.1에서 구현했고, 소비자는
  Playable Session slice의 포화 테스트가 필요를 증명할 때 추가한다.

metric:

- `snf::runtime::Distribution`이 log-linear bucket으로 표본을 집계하고 p50/p95/p99/max를 노출한다.
  record는 대기하지 않으므로 snapshot을 읽는 쪽이 Worker를 지연시키지 않는다. 표현 범위
  (`REPRESENTABLE_UPPER_BOUND`, 약 17초) 안에서 percentile은 표본이 속한 bucket의 상한이어서 실제 값보다
  작아지지 않고 최대 12.5% 크다. 그 범위를 넘는 표본은 percentile이 상한에서 포화하므로 max만 정확하게
  보고한다. 즉 과도한 지연은 p99가 아니라 p99보다 훨씬 큰 max로 드러난다.
- 기록이 멈춘 뒤의 snapshot은 sample_count와 max가 정확하다. 기록 중 snapshot은 bucket을 하나씩 읽는
  lock-free 근사 관측값이며 한 시점의 일관된 사진이 아니다.
- Actor command queue wait를 Worker별로 `ActorRuntimeWorkerStats::queue_wait_nanoseconds`로 노출한다.
  평균은 제거하고 max는 유지한다. control submission은 command 계정과 마찬가지로 표본에서 제외한다.
- reactor turn 지연을 `TcpServerMetrics::reactor_turn_nanoseconds`로 노출한다. 표본은 `epoll_wait`
  반환부터 그 회차 event 처리 종료까지이며, ready event가 없는 회차는 표본에 넣지 않아 유휴 대기가
  분포를 희석하지 않는다.
- 연결별 pending send 분포와 outbound queue depth 분포, 그리고 현재 연결 수, pending send 연결 수와
  outbound depth gauge를 함께 노출한다.
- outbound hand-off 시간을 `TcpServerMetrics::outbound_queue_wait_nanoseconds`로 노출한다. Logic
  Worker가 `OutboundAction`을 게시한 시점부터 reactor가 그것을 소비한 시점까지이며, 포화 시 blocking
  대기를 포함한다. 게시 시점은 sink가 기록하므로 게시하는 runtime은 이 metric을 알지 않는다.
  이 수치는 4.1이 outbound 구조를 바꾸기 전의 현재 구조 기준선이다. 4.1 이후에는 blocking 대기가
  없으므로 같은 metric이 commit부터 소비까지만 재고, 용량 대기는 Actor의 suspend로 드러난다.
- `GameServerConfig::metrics_report_interval`과 `metrics_reporter`로 운영 중 주기 노출 경로를 갖는다.
  reactor가 epoll timeout을 이 주기로 제한하므로 유휴 상태에서도 보고가 나온다. 종료 후에는
  `GameServer::getMetricsSnapshot()`이 같은 표면을 제공한다.
- `metrics_reporter`는 reactor thread에서 호출되므로 block하면 안 된다. 파일이나 네트워크 전송이
  필요하면 별도 bounded logger queue에 게시하고 전송은 그 소비자가 담당한다. 보고 비용은 turn 측정
  이후에 발생하므로 `reactor_turn_nanoseconds`에 포함되지 않는다. 이 metric은 turn당 reactor 작업량이며
  reactor 점유율 전체가 아니다.
- baseline은 `snf_load_client` 부하 실행 중의 주기 보고와 종료 요약으로 수집한다. 향후 Runtime
  최적화를 선택하면 reactor turn 지연, Actor queue wait, outbound hand-off 시간과 outbound
  queue depth를 같은 조건에서 비교한다.

blocking outbound는 실행 pool을 통합할 경우의 blocker였다. 통합 후에는 outbound를 비우는 주체와
대기하는 주체가 같은 pool에 있으므로, blocking 대기를 남겨두면 pool 전체가 진행하지 못한다. 단계
4.1에서 제거했다.

완료 기준:

- 현재 포화 동작과 목표 포화 동작이 아키텍처 초안 §8에 현재/목표/예정 단계로 기록된다.
- in-flight credit과 command terminal 정의가 문서에 고정되고 4.1과 Playable Session slice가 이를
  참조한다.
- baseline metric을 부하 테스트에서 수집할 수 있고 통합 전후 비교의 기준선이 된다.
- 포화 동작, FIFO, fairness, lifecycle retry와 drain/cancel/failure 의미가 변하지 않는다.

## 4.0. Actor Coroutine: Suspend와 Resume (완료)

- 일반화된 Actor binding의 첫 적용으로 `PlayerActor` handler 반환형을
  `ActorTask<PlayerResult>`로 바꾼다.
- 외부 command ingress와 Worker별 내부 continuation queue를 분리한다.
- async operation 시작 전에 in-flight와 terminal continuation capacity를 함께 예약한다.
- 외부 Worker는 coroutine을 직접 resume하지 않고
  `{ActorKey, ActorIncarnation, TaskId}`가 포함된 결과를 원래 Actor Worker로 게시한다.
- suspend 중 같은 Actor의 일반 command는 mailbox에서 기다리고 다른 Actor는 계속 진행한다.
- 취소와 late completion은 `ActorIncarnation + TaskId`로 구분한다.
- graceful drain은 외부 입력, mailbox, ready/running/suspended task와 continuation을 모두 관찰한다.
- cancel 요청과 coroutine resume/frame 파괴는 owning Worker에서만 실행한다. terminal claim을 먼저 얻은
  completion은 result 저장과 예약된 continuation publish까지 중단 없이 끝내며, cancel은 그 publish를
  기다린다.
- suspended command의 queue wait, outstanding과 processed 회계를 terminal 시점까지 정확히 한 번
  유지한다.
- production `PlayerActor`는 coroutine 반환형으로 전환됐지만 PING에는 await할 작업이 없어 첫 resume에서
  동기 완료한다. 첫 production suspension point는 4.1의 outbound reservation이다.

완료 기준:

- suspend 중 다른 Actor가 진행하고 원래 Actor는 terminal continuation 이후에만 재개된다.
- 성공·실패·취소 중 terminal continuation이 정확히 하나만 관측된다.
- completion/cancel, completion/shutdown, 즉시 completion, double completion, deadline 뒤 late
  completion, reservation 포화와 마지막 continuation/drain 경합이 테스트된다.
- resume과 coroutine frame 파괴가 owning Worker에서만 일어난다.
- suspend 전·후 예외 전파와 Debug, ASan·UBSan, TSan 검증이 통과한다.

완료 시 release 부하 측정(200 connections, 12s, 20 req/s)에서 48,000/48,000 응답, timeout·queue
overflow 0을 기록했다. Actor queue wait `p50/p95/p99/max`는 Worker별
`24575/65535/114687/398125`, `26623/81919/131071/406875` ns였고, client RTT는
`p50 2.748 ms / p95 3.939 ms / p99 4.689 ms`였다. 3.9 baseline의 queue wait p99
`360447 ns`, RTT p99 `4.906 ms`보다 악화되지 않아 동기 완료 fast path는 추가하지 않는다.

## 4.1. Async Outbound Reservation (완료)

Phase 4.0의 첫 production awaiter로 blocking outbound publish를 제거했다.

- `OutboundChannel`이 action 저장과 용량 회계(`queued + reserved ≤ capacity`)를 하나의 동기화 경계에
  둔다. 회계를 별도 객체로 분리하면 pop과 commit 사이에서 두 값이 어긋난다.
- `OutboundReservation`은 move-only RAII 토큰이며 commit되지 않은 슬롯을 소멸자가 반환한다. 취소에
  terminal claim을 빼앗긴 grant, 파괴된 coroutine frame, 예약보다 적게 방출한 handler가 모두 별도
  경로 없이 용량을 되돌린다.
- Binding은 command를 `Handling → Reserving` 두 단계로 실행한다. handler task가 결정을 끝낸 뒤에야
  binding이 용량을 얻고 방출하므로, `PlayerActor`와 `PlayerResult`는 outbound 용량이 유한하다는
  사실을 알지 않으면서도 effect 적용은 handler 정상 반환 이후로 유지된다.
- 포화가 아니면 `tryReserve`가 성공하고 operation을 아예 시작하지 않는다. in-flight slot,
  continuation, suspend가 모두 없으므로 비포화 왕복의 비용 구조는 4.0과 같다. 포화일 때만 예약
  task가 만들어져 그 Actor 하나가 suspend된다.
- 예약 await는 coroutine frame 안의 guard로 waiter를 회수한다. suspend된 frame을 파괴할 때도 그
  guard가 실행되므로 per-operation cancel과 runtime 전체 cancel 모두 registry를 비운다.
- grant는 reactor만 수행한다. 용량을 되돌린 Worker는 grant하지 않고 wake-up만 신호하므로 grant
  작업량이 reactor 회차당 상한 안에 머문다. 한 회차에 검사·승인하는 waiter 수를 모두 제한하고,
  대상 상한에 막힌 연결은 뒤로 회전시켜 다른 연결을 막지 않는다.
- 연결별 상한(`max_outbound_slots_per_connection`)으로 한 연결이 공유 용량 전체를 점유하지 못하게
  한다. 연결별 회계는 command 단위로 만들고 버리지 않고, reactor가 연결을 수락할 때 만들고 연결이
  사라진 뒤 보유분이 빠지면 정리한다. reactor가 추적하지 않는 연결의 회계는 — session이 닫힌 뒤
  도착한 command가 만든 것이므로 앞으로 정리 신호가 오지 않는다 — 비는 즉시 제거한다. 그래서 연결
  churn이 항목을 쌓지 않는다. `tracked_outbound_connections`는 그 회계가 command 발생률이 아니라
  살아 있는 연결 수를 따라가는지 관측한다.
- 예약 대기 자체가 불가능한 경우(Worker in-flight 예산 소진)에는 응답을 조용히 버리지 않고 연결을
  `ConnectionCloseCause::Overflow`로 종료한다. 종료에는 outbound 용량이 필요하지 않으므로 용량이
  없는 연결에도 적용할 수 있는 정책이다. 종료 요청은 연결로 식별해 합치므로 한 연결이 반복 실패해도
  다른 연결의 종료를 밀어내지 않는다. 기록 상한은 현재 연결 수와 연결 종료 전에 이미 승인된 Actor
  command 상한을 합쳐 잡는다. 그래도 상한에 도달하거나 기록 allocation이 실패하면 Worker에서 던지거나
  조건을 버리지 않고 할당 없는 fail-safe flag를 세운다. reactor는 이를 소비해 현재 session을 모두
  Overflow로 닫으므로, 정확한 연결 ID를 보존하지 못한 경우에도 응답이 조용히 사라지지 않는다.
- 한 command의 effect 수가 연결별 상한보다 크면 예외가 아니라 command 단위 거부로 처리한다.
  `canEverReserve`가 그것을 포화와 구분하므로 영원히 오지 않는 grant를 기다리지도 않고, 결과 하나
  때문에 그 Worker의 모든 Actor가 함께 죽지도 않는다. 처리는 위와 같은 연결 종료다.
- command의 신원별 신호를 두 가지로 나눈다. **release**는 credit 반환 신호이며 binding이 만든 모든
  submission에서 정확히 한 번 발생한다. scheduler가 승인된 submission을 성공·예외·취소·mailbox
  폐기·ingress 거부 어느 경로에서도 정확히 한 번 파괴하기 때문이고, ingress가 거부한 post도 이
  경계에서 이미 credit을 취득했으므로 반환해야 한다. **admission rejection**은 그 거부 자체이며
  ingress가 결과를 보고 직접 보고한다. 원인이 다른 사실이므로 실행된 command 수에 합산하지 않는다.
  따라서 `command_terminals`는 승인되어 결과에 도달한 command만 세고, 거부는
  `command_admission_rejections`로 따로 노출한다.
- release 토큰은 command를 승인하는 binding 경계에서 무장하므로, protocol 단계에서 거부된 frame은
  아무 신호도 내지 않는다. 향후 credit을 추가하면 취득도 같은 경계여야 두 수치가 일치한다.

완료 기준 결과:

- Logic Worker는 outbound 포화로 대기하지 않는다. 포화는 Actor suspend로만 나타난다.
- outbound capacity를 1로 강제한 통합 테스트에서 모든 응답이 순서대로 도착하고 graceful shutdown이
  완료된다. capacity 1 단위 테스트에서 effect 순서와 handler 원자성이 유지된다.
- 같은 Actor의 다음 command는 이전 command가 terminal에 도달한 뒤에만 dispatch되므로 미방출 effect를
  앞지르지 않는다.
- release가 성공·취소·mailbox 폐기·ingress `Full`·ingress `Closed`에서 각각 정확히 한 번 관측되고,
  그중 `Full`과 `Closed`는 terminal이 아니라 admission rejection으로 집계된다. 부하 실행에서 48,000
  command에 대해 terminal 48,000건, admission rejection 0건이다.
- 한 연결이 점유할 수 있는 공유 용량은 `max_outbound_slots_per_connection`(기본 64/4096)으로,
  메모리는 그 위에 session별 `max_pending_send_bytes`로 각각 상한이 있다.

측정 결과. 4.0과 3.9 항목에 기록된 수치는 다른 환경에서 측정했으므로 비교 대상으로 쓰지 않고, 같은
환경(Docker on macOS)에서 4.0 커밋을 재측정해 함께 적는다. 조건은 동일하게 200 connections, 12s,
20 req/s이며 각 3~4회 실행 범위다.

| 지표 | 4.0 재측정 | 4.1 |
| --- | --- | --- |
| 응답 | 48,000/48,000, timeout 0 | 48,000/48,000, timeout 0 |
| client RTT p50 | 2.52–2.59 ms | 2.46–2.59 ms |
| client RTT p99 | 4.28–5.58 ms | 3.81–4.13 ms |
| Actor queue wait p50 | 18–23 µs | 27–37 µs |
| Actor queue wait p99 | 106–246 µs | 164–393 µs |
| outbound hand-off p99 | 983 µs | 918 µs |

4.1 쪽 4회 중 1회는 RTT p99 `12.995 ms`, Actor queue wait max `11.5 ms`를 기록했다. 두 Worker가 같은
시점에 같은 크기로 튀었고 4.0 재측정에서도 reactor turn max가 `34 ms`까지 나온 실행이 있으므로 이
환경(가상화된 Docker) 자체의 정지로 보고 위 범위에서 제외했다. 재현되지 않았다.

`outbound_queue_wait_nanoseconds`의 의미가 이 단계에서 바뀐다. 4.0에서는 포화 시 blocking 대기를
포함했고 4.1에서는 commit부터 소비까지만 재므로, 용량 대기는 Worker별
`suspend_duration_nanoseconds`로 따로 드러난다. 다만 이 부하는 기본 capacity 4096에서 outbound
depth 최대 192에 머물러 포화가 발생하지 않았고, 그래서 `suspended_commands`가 0이며 두 수치를 그대로
비교할 수 있다. 예약 경로 자체의 동작은 capacity 1 테스트가 담당한다.

Actor queue wait p50이 남은 차이는 command당 lock 획득이 하나 늘어난 비용이다. 첫 측정에서는
연결별 회계를 command마다 만들고 버려 p50이 3배까지 벌어졌고, 회계를 연결 수명으로 옮기고 reactor
드레인을 batch로 바꿔 대부분을 회수했다. RTT는 동등하므로 fast path를 lock-free로 만드는 작업은
하지 않았다. 향후 측정이 Runtime 최적화를 정당화할 때만 이 경로를 다시 구성한다.

## 4.2. Network Correctness (완료)

현재의 단일 `epoll` reactor와 callback 구조를 유지하고 관측된 correctness 문제만 해결한다.

- `FrameDecoder`를 `push(bytes) + tryDecodeNext()` pull API로 바꾸고 `append()`는 호환 wrapper로
  남긴다. 배치 뒤의 잘못된 frame이 앞의 정상 frame까지 없애는 경로를 제거한다.
- protocol error 후 buffer 정리, partial frame, 여러 frame 순서와 호환 wrapper를 단위 테스트한다.
- 현재 inbound `Full → Overflow close`, lifecycle retry, Session send bound와 shutdown 동작은
  변경하지 않는다.
- `NetExecutor`, network coroutine, heartbeat와 범용 deadline heap은 이 단계의 범위가 아니다.

완료 기준:

- 오류 앞에 완전히 decode된 frame이 순서대로 반환된다.
- 오류 후 decoder 상태가 명시적이고 buffer가 무제한 유지되지 않는다.
- 기존 Debug 단위·통합·부하 테스트가 통과한다.

완료 결과:

- pull API는 frame을 한 번에 하나씩 반환하고 partial frame은 다음 `push()`까지 보존한다.
- protocol error는 decoder buffer를 초기화한다. 호환 `append()`는 같은 배치에서 오류보다 앞선 정상
  frame을 반환하며, 오류 뒤 decoder 재사용도 단위 테스트로 검증한다.
- Debug, ASan·UBSan과 TSan의 단위·통합·부하 테스트가 모두 통과했다.

## 보류된 Runtime 최적화 설계: ConnectionScope

> 현재 콘텐츠 로드맵의 선행 조건이 아니다. Playable Session과 Zone 부하에서
> cross-runtime hand-off, reactor turn 지연, lifecycle 조정 또는 shard 편향이 실제 병목임이
> 증명될 때 아래 계약을 실행 계획으로 다시 승격한다.

연결 하나의 네트워크 상태와 수명을 장수명 coroutine 쌍으로 표현한다. 게임 상태는 여전히 Actor가
소유하고 `ConnectionScope`는 네트워크 상태만 소유한다.

현재 `TcpServer::run()`은 coroutine을 재개하는 executor가 아니라 동기 epoll 루프다. 따라서 이 단계의
산출물에는 최소 Executor가 포함된다.

- 기존 reactor loop에 최소 Executor adapter와 ready queue를 추가한다.
- I/O readiness는 coroutine을 inline resume하지 않고 ready queue에 게시한다.
- async read/write awaiter와 runtime deadline primitive를 제공한다.
- 새 thread pool은 만들지 않으며 기존 reactor thread가 executor를 실행한다.
- 이 Executor 계약과 ready queue는 선택적 UnifiedRuntime 통합에서 그대로 재사용한다.

그 위에 다음을 올린다.

- read loop과 write loop을 분리하고 Session 상태를 소유권으로 나눈다. read loop은 recv buffer, frame
  assembler와 in-flight credit을, write loop은 send buffer와 pending send 상태를 소유한다. 공유하는
  것은 종료 상태 하나로 줄인다.
- credit이 소진되면 socket 읽기를 중지하고, 4.1의 command terminal 신호로 credit을 반환해 재개한다.
  4.1은 그 신호를 command를 승인하는 binding 경계에서 생산하므로 credit 취득도 같은 경계여야 한다.
  protocol 단계에서 거부되는 frame은 command가 된 적이 없으므로 credit을 소비하지 않는다.
- read loop, write loop, deadline과 control이 모두 `requestClose(cause)`를 호출할 수 있고,
  `ConnectionScope`만 단일 terminal 전이를 수행해 `ConnectionClosed`를 최대 한 번 게시한다. ingress close
  이전에 종결에 진입했으면 정확히 한 번이며, graceful shutdown도 먼저 활성 scope를 종결한다.
- `ConnectionScope`는 새 read admission을 중단하고, read loop이 진행 중인 ingress 게시를 마친 뒤 같은
  ordered ingress에 `ConnectionClosed`를 게시한다. 따라서 terminal 전이 전에 승인된 그 연결의 command
  보다 뒤에 위치한다.
- heartbeat와 idle timeout은 runtime deadline primitive로 표현하며 gameplay domain `TimerService`에
  의존하지 않는다.

착수 전 [ConnectionScope 계약](./connection-scope-contract.md)에 Executor 계약, 상태 소유권, 단일 종결,
close 순서와 취소 전파를 고정했다. 그 문서가 고정한 결정은 다음과 같다.

- ready queue는 `coroutine_handle`이 아니라 `{ConnectionId, Side, SuspensionGeneration}`을 저장하고 drain
  시점에 resolve한다. token은 값이므로 무효 포인터가 되지 않고, 유효성은 scope 존재·side active·awaiter
  queued·generation 일치를 모두 요구한다. `Closing` scope도 map에 남으므로 scope 존재만으로는 판정이 되지
  않는다. 논리적 무효화로 충분하므로 물리 token이 남아 있어도 retire를 허용한다. 상한은
  `2 × max_scopes`이며 고정 용량, checked multiplication, 초과는 불변식 위반이다. 다만 use-after-free와
  용량 회계는 다른 문제이므로 ring slot 회계는 scope 수명이 아니라 token의 물리 제거에 붙인다. retire
  시점에 남은 token은 orphan으로 세고 새 scope 수락 조건에 포함되므로, retire가 slot을 조용히 반환해
  상한을 넘기는 경로가 없다.
- credit state의 소유 참조는 reactor 하나만 보유하고 release 토큰은 `outstanding` counter만 증감한다.
  일반적인 refcount로 두면 마지막 참조를 버리는 주체가 Worker가 되어 deallocation이 release 경로에서
  일어난다. reactor가 `detached && outstanding == 0`을 관측한 뒤 자기 turn에서 회수한다. 취득 경계는
  release 토큰이 무장되는 binding 경계 그대로이며, 그 경로를 지나는 admission context는 routing DTO까지만
  가고 `PlayerCommand`나 Actor 상태에는 들어가지 않는다.
- `ConnectionClosed`는 graceful shutdown을 포함해 ingress close 전에 종결된 연결마다 exactly-once다.
- 종결 시 취소 범위는 cause가 결정한다. 일반 종료는 read/write를 모두 취소하고, `ServerShutdown`은 read만
  취소해 Logic drain과 send queue empty 또는 grace deadline까지 write-side drain을 유지한다. 그래서 이미
  승인된 command의 응답이 종료 중에도 나간다.
- deadline heap은 indexed heap이며 cancel·update가 물리 node를 즉시 제거한다. scope·목적당 등록된
  deadline이 1개라는 규칙은 등록 수만 제한하므로, 긴 deadline + 연결 churn에서 무효화된 물리 node가 쌓이는
  경로를 따로 막아야 한다. 활동마다 재무장하지 않고 `last_activity_at`을 검사해 만료 시 한 번 재무장하므로
  heap push는 frame당이 아니라 timeout 간격당 1회다.
- heartbeat는 하나의 deadline handle을 상태마다 갱신하는 `Idle → Queued → Awaiting` 상태 기계다.
  outstanding Ping은 최대 1개이고, response timeout은 enqueue가 아니라 실제 송신 완료 시점부터 세며,
  interval은 `Awaiting` 해제 시점부터 다시 센다. `Queued`에도 send timeout을 둔다.
  `max_pending_send_bytes`는 공간 상한이지 진행성 보장이 아니므로, 상대가 읽지 않아 Ping 하나가 queue에
  남으면 그 상한에 닿지도 않고 송신 완료도 오지 않아 상태가 영구히 머문다.
- `ConnectionCloseCause::Timeout`을 추가한다. idle timeout과 heartbeat 미응답이 이 cause를 공유한다.
- heartbeat는 네트워크 계층에서 완결한다. write loop이 outbound channel 밖으로 `Ping`을 방출하고 read
  loop이 활성 시에만 `Pong`을 가로채므로 command도 credit도 소비하지 않는다. 기본값은 비활성이며 그때
  inbound `Pong`은 현재의 `ProtocolError` 종료 동작을 그대로 유지한다.
- 별도 `RuntimeId::Net`은 추가하지 않는다.

완료 기준:

- inbound frame을 조용히 드롭하지 않고 credit 소진 전에 읽기를 중지한다.
- 종료 원인을 누가 먼저 관측하든, ingress close 이전에 종결에 진입한 연결의 `ConnectionClosed`가 정확히
  한 번, terminal 전이 전에 승인된 command 뒤에 게시된다. graceful shutdown은 활성 연결의 lifecycle
  승인과 Player snapshot 저장 뒤 Logic ingress를 닫는다.
- coroutine frame 파괴, socket close와 deadline 만료가 경합해도 use-after-free가 없다.
- partial send와 `EPOLLOUT` 재무장이 write loop 안에서 처리된다.
- Debug, ASan·UBSan, TSan에서 연결 폭주와 강제 종료 시나리오가 통과한다.

### UnifiedRuntime 통합

Connection, I/O continuation, Actor turn과 timer를 하나의 Worker Pool에서 실행한다. 실행 pool을
통합하는 것이며 게임 상태를 공유하는 것이 아니다. Actor의 단일 소유권과 순차 실행은 그대로 유지한다.

선행 조건:

- Logic Worker의 blocking outbound 제거 (4.1, 완료)
- continuation capacity 예약 (4.0)
- task, recv, send와 actor turn budget 정의
- graceful drain 계약 (`docs/runtime-lifecycle-contract.md`)
- baseline metric 확보 (3.9)

- Actor 실행은 고정 shard 대신 Actor별 직렬화 규칙 위에서 이루어진다. 같은 `ActorKey`의 command가
  동시에 실행되지 않는다는 불변식은 유지한다.
- 전체 종료 판정을 `UnifiedRuntimeDrained`로 재조립한다.

```text
UnifiedRuntimeDrained =
    external ingress closed
    ∧ ready queue empty
    ∧ no running tasks
    ∧ no suspended operations
    ∧ no pending I/O completion
    ∧ no pending deadline
```

완료 기준:

- 같은 `ActorKey`의 handler가 동시에 실행되지 않는다.
- 긴 Actor turn 때문에 네트워크 I/O가 무기한 지연되지 않도록 budget이 동작한다.
- 3.9 baseline과 비교해 `outbound_queue_wait_nanoseconds` 감소를 수치로 제시하고, reactor turn 지연,
  Actor queue wait과 end-to-end RTT의 변화를 함께 제시한다.
- drain, cancel과 failure가 통합 후에도 정의된 순서로 동작한다.

## 5. Playable Player Session Vertical Slice

하나의 Player가 식별되고, 로드되고, Zone에 들어가 이동한 뒤 저장·복원되는
최소 플레이 흐름을 end-to-end로 완성한다.

### 5.1 Identity와 Session attach (완료)

- 개발용 인증 경계에서 영속 `PlayerId`를 만들고 `ConnectionId`, `ProvisionalActorId`와
  서로 다른 타입과 용도로 유지한다.
- 동시 로그인, attach/detach/logout, Actor incarnation과 passivation 정책을 고정한다.

완료 결과:

- `Authenticate`는 non-zero `PlayerId`를 8-byte big-endian payload로 받고 `Authenticated`로
  응답한다. 같은 연결·같은 ID의 재요청은 멱등하고, 한 Player의 동시 연결은 먼저 attach한 연결이
  이기며 뒤 연결은 protocol rejection으로 닫힌다.
- PING은 pre-auth health 경로로 유지한다. 다만 provisional Actor에 command를 승인한 연결은 같은
  연결에서 뒤늦게 인증할 수 없다. 인증을 첫 actor-bound frame으로 제한해 한 연결이 provisional과
  persistent Player Actor를 동시에 만들지 않는다.
- persistent route는 `ActorKind::Player`와 `PlayerId`를 사용한다. 연결 close가 ingress에 수락되면
  Session은 `Closing`으로 남고, owning Worker가 Actor slot을 실제 제거·파괴한 뒤에만 detach된다.
  따라서 재접속 auth가 이전 close 뒤 mailbox에 들어갔다가 eviction과 함께 버려지지 않는다.
- 이 slice의 logout은 socket disconnect가 만드는 ordered `ConnectionClosed` lifecycle이다. 응답을
  보장하는 명시적 logout command는 gameplay 규칙이 필요할 때 별도로 추가한다. disconnect마다 즉시
  passivate하고, 같은 `PlayerId`의 재접속은 scheduler가 새 `ActorIncarnation`으로 활성화한다.
- 단위 테스트는 ID wire 변환, attach 충돌·rollback·Closing을 검증하고, 통합 테스트는 auth, persistent
  PING, 중복 로그인 거부, disconnect/passivation과 같은 PlayerId 재접속을 검증한다.

### 5.2 Async repository와 reconnect

- repository interface와 결정적 fake로 Player load/save 흐름을 먼저 완성한다.
- blocking DB adapter는 bounded DB Worker Pool에 연결하고 Player Actor coroutine만 suspend한다.
  DB Worker는 coroutine handle, Actor 포인터나 runtime의 mutable 상태를 보유하지 않는다.
- disconnect/save/passivation 후 reconnect가 같은 `PlayerId`의 상태를 복원한다.
- 느린 load/save 중 한 연결이 다른 연결의 mailbox를 압박하는지 측정한다. 재현되면
  4.1의 terminal 신호를 소비하는 per-connection credit을 **현재 reactor**에 추가한다.

#### 5.2a Repository 경계와 reconnect 복원 (완료)

- `PlayerRepository`는 value record와 completion callback만 받으며 Actor, coroutine handle과 runtime
  mutable state를 받지 않는다. `ThreadedPlayerRepository`는 non-blocking bounded FIFO 뒤의 전용
  Worker에서 job을 실행하고 queue depth, high-water mark와 admission rejection을 노출한다.
- 이 단계의 저장 backend는 결정적 in-memory store였다. 6.3에서 MySQL/InnoDB adapter를 추가했으며,
  repository queue와 Actor continuation 계약은 그대로 유지한다.
- 첫 persistent command는 load completion까지 해당 Player만 suspend한다. `ConnectionClosed`는 save
  completion 뒤에만 evict하고, reconnect activation은 저장된 `handled_command_count`를 복원한다.
- 느린 load를 고정한 단일 Worker 테스트에서 같은 Worker의 provisional Player가 PING을 먼저 완료한다.
  두 차례 auth/PING/disconnect 통합 테스트는 저장 값이 `2 → restore → 4`로 이어짐을 검증한다.
- repository unavailable은 load 결과로 명시되어 연결 종료를 요청하고, save unavailable은 영속 상태
  유실을 숨기지 않고 runtime failure로 승격한다. 실제 DB adapter의 재시도/idempotency 정책은
  6.3에서 transaction 의미와 함께 확정했다.

### 5.3 Minimal Zone

- 일반화된 scheduler에 `ZoneActor`와 typed Zone binding을 추가한다.
- enter/leave/move, 단일 Zone의 결정적 tick과 최소 AOI를 구현한다.
- `RouteCoordinator`가 `SessionRoute`와 `route_epoch`을 소유하고 stale destination을 거부한다.
- domain `TimerService`는 주입 가능한 Clock, timer identity, cancel과 late event 폐기를 제공하고
  owning Worker의 mailbox에 typed tick event를 게시한다.

#### 5.3a Deterministic Zone Actor와 binding (완료)

- `ZoneActor`가 enter/leave/move, player별 `route_epoch`, 결정적 simulation tick과 최소 원형 AOI를
  단독 소유한다. stale route와 stale tick은 상태를 바꾸지 않으며 AOI 결과는 `PlayerId` 오름차순이다.
- `ZoneActorBinding`과 `ZoneActorIngress`가 typed `ZoneCommand`를 기존 `ActorKind::Zone` shard에
  올린다. Player binding과 같은 mailbox, turn budget, Worker affinity와 passivation 규칙을 쓴다.
- 위치 극값에서도 거리 제곱이 overflow하지 않도록 축 범위를 먼저 검사하고, 상태 전이와 AOI 경계를
  단위 테스트한다. binding 테스트는 enter → move → passivate FIFO와 owning Worker 실행을 검증한다.
- network route, `RouteCoordinator`와 결과 sink는 5.3b에서 연결했다. `TimerService`는 다음 5.3
  하위 단계다. domain Actor는 어느 단계에서도 `ConnectionId`, wire frame이나 timer thread를 직접
  알지 않는다.

#### 5.3b Authenticated Zone route와 wire slice (완료)

- 인증된 session만 `EnterZone`을 보낼 수 있고, `Move`와 `LeaveZone`은 reactor 소유
  `RouteCoordinator`의 현재 `SessionRoute`를 통해 typed Zone command로 변환된다. 같은 Zone 재입장은
  멱등하고 다른 Zone으로의 즉시 재입장은 거부하며, player별 `route_epoch`은 route를 다시 만들 때마다
  단조 증가한다.
- initial enter route는 command ingress 승인 전에 임시 등록하고 `Full`, `Closed` 또는 예외면 rollback한다.
  명시적 leave와 disconnect는 같은 Zone mailbox에 `LeaveZoneCommand`를 먼저 승인한 뒤 route를
  제거한다. 5.3b 완료 시점에는 여러 Zone 사이 handoff의 이전 destination 정지 → 새 destination 활성화 →
  route 공개 protocol이 없어 cross-zone enter를 거부했다. 이 제한은 7.4에서 bounded completion과 보상
  상태 기계를 연결해 해제했다.
- `ProtocolZoneResultSink`가 Zone 결과를 bounded wire payload로 바꾸고 기존 outbound reservation을
  사용한다. 즉시 용량을 얻지 못하면 응답을 버리지 않고 해당 연결의 admission failure를 reactor에
  보고한다. client-originated Zone command도 Player command와 같은 exactly-once terminal signal과
  별도 admission rejection 회계에 참여하며, disconnect가 만든 내부 leave에는 client credit을 만들지
  않는다.
- 단위 테스트는 route 멱등성·epoch·rollback, wire result와 outbound 포화, Zone binding의 terminal
  회계를 검증한다. TCP 통합 테스트는 authenticate → enter → move → leave 왕복과 epoch/좌표 보존을
  실제 socket으로 검증한다.
- tick 실행 시간·overrun 지표는 5.3f에서 완료했다. 이동 최신값 coalescing은 playable 기준선에서
  queue overflow나 tick overrun이 재현되지 않아 선행 구현하지 않고, 단일 hot Zone 측정이 필요를
  증명할 때 진행한다.

#### 5.3c Domain Zone TimerService (완료)

- `ZoneTimerService`는 `steady_clock` 기반 production clock과 주입 가능한 `TimerClock`, 단조 증가
  `TimerId`, Zone별 periodic record와 명시적 cancel을 제공한다. 별도 timer thread는 Zone 상태를
  만지지 않고 `ZoneActorIngress`에 `ArmZoneSimulationTimer`, `ZoneSimulationTick`,
  `CancelZoneSimulationTimer` value command만 게시한다.
- 첫 route가 생기기 전에 Arm을 같은 Zone mailbox에 승인하고 마지막 route의 Leave 승인 뒤 Cancel을
  게시한다. timer 생성 뒤 Enter가 거부되면 route와 timer를 함께 rollback하며, cancellation command가
  mailbox 포화로 거부되면 bounded timer record를 유지해 재시도한다.
- ZoneActor는 현재 `TimerId`를 소유한다. timer 교체·취소 뒤 도착한 이전 id의 tick은 `StaleTimer`로
  폐기하므로 service record 제거와 Actor 처리 사이에 value command가 남아도 UAF나 상태 변경이 없다.
- 한 Zone이 여러 interval 늦어져도 회차당 tick 하나만 게시하고 놓친 interval은
  `skipped_intervals`로 집계한다. mailbox `Full`은 `dropped_full`, 성공은 `fired`, active/cancel-pending
  수와 lifecycle/failure도 metric으로 노출한다. timer thread 예외는 Logic runtime failure wake-up을
  일으키고 `GameServer::run()` join에서 원인을 재throw한다.
- 결정적 clock 테스트는 idempotent 등록, capacity, no-catch-up, cancel retry, disable과 failure 전파를
  검증한다. TCP 통합 테스트는 실제 timer thread가 enter 뒤 tick을 게시하고 마지막 leave 뒤 timer를
  취소하는지 검증한다.

#### 5.3d Zone 위치 save/reconnect restore (완료)

- `PlayerRecord`가 선택적 `PlayerLocation{ZoneId, ZonePosition}`을 저장한다. PlayerActor는 repository
  load에서 이를 복원하고 disconnect close payload의 location을 적용한 뒤 기존 async save/passivation
  경로로 함께 저장한다. 명시적 `LeaveZone`은 location을 지워 다음 입장의 client 좌표를 사용한다.
- reactor의 session directory는 ZoneActor 상태를 읽지 않고 **Actor ingress가 승인한** enter/move의
  마지막 위치만 기록한다. 같은 route의 Zone command는 한 mailbox에서 FIFO이므로 disconnect가 leave를
  승인하기 직전에 캡처한 값은 그 leave 앞에서 최종 적용될 위치다. 이 value snapshot을 Player close에
  실어 서로 다른 Zone/Player Worker 사이 동기 대기나 mutable 상태 참조를 만들지 않는다.
- repository load callback은 owning Player Worker에서 immutable location value만 session directory로
  돌려준다. 인증 응답은 callback 뒤에 방출되므로 client가 응답을 받은 뒤 보내는 `EnterZone`은 같은
  Zone이면 저장 위치를 사용하고, 다른 Zone이면 요청 위치를 사용한다.
- close snapshot은 `unknown`과 authoritative `none`을 구분한다. DB load가 끝나기 전 disconnect는
  `unknown`으로 Player mailbox에 들어가 load가 복원한 위치를 유지하고, load 완료나 명시적 leave 뒤의
  `none`은 실제로 Zone 밖이라는 값으로 저장한다.
- 단위 테스트는 repository round-trip, Player snapshot, session location, explicit leave clear와 close
  value routing을 검증한다. TCP 통합 테스트는 authenticate → enter → move → disconnect/save → 같은
  PlayerId reconnect/load → enter가 client의 새 좌표 대신 저장된 좌표를 반환하는 전체 흐름을 검증한다.

#### 5.3e Empty Zone passivation (완료)

- 마지막 route의 Leave와 timer Cancel은 같은 Zone mailbox에 순서대로 들어간다. ZoneActor가 player 0,
  active timer 없음이 된 turn에서 binding이 `PassivateIfIdle`을 반환하고 runtime은 그 시점 mailbox까지
  비어 있을 때만 slot을 제거한다.
- lifecycle fence인 기존 `Evict`는 Player close처럼 mailbox tail을 의도적으로 폐기한다.
  `PassivateIfIdle`은 이미 승인된 tail이 있으면 activation을 유지하고 계속 처리한다. 따라서 Cancel 뒤
  새 Arm/Enter가 queue된 재입장 경쟁에서 새 route가 사라지지 않는다.
- binding 테스트는 정상 Leave → Cancel 뒤 빈 Zone 제거와, 첫 dispatch를 고정한 상태에서
  Leave/Cancel → Arm/Enter 재입장 tail을 미리 승인해 8개 command가 모두 처리되고 최종 빈 상태에서만
  한 번 evict되는지를 검증한다. disconnect/save/reconnect TCP 테스트도 Player와 Zone slot이 모두
  제거된 뒤 재접속한다.

#### 5.3f Zone execution metric (완료)

- `ZoneActorBinding`은 모든 Zone command와 `ZoneSimulationTick` handler 실행 시간을 기존 lock-free
  fixed-bucket distribution에 기록해 `p50/p95/p99/max`로 노출한다. 설정 가능한 `zone_tick_budget`
  이상 걸린 tick은 `tick_overruns`에 별도 집계한다.
- metric은 binding 단위 atomic surface라 여러 Zone Worker가 동시에 기록해도 Worker를 block하지 않는다.
  기존 Worker별 accepted/processed, actor count와 queue wait 분포를 함께 보면 shard 편향과 실행 비용을
  구분할 수 있고, TimerService의 `skipped_intervals`/`dropped_full`과 결합하면 scheduler 지연과 handler
  비용도 구분할 수 있다.
- zero-budget 결정적 테스트는 command 3개, tick 1개와 overrun 1회를 정확히 검증한다. TCP Zone
  통합 테스트도 production timer가 게시한 tick 표본이 binding metric에 나타나는지 확인한다.

완료 기준:

- 인증·load·attach·Zone 입장·이동·disconnect/save·reconnect/restore가 하나의 통합 테스트로
  검증된다.
- 같은 Player/Zone의 상태는 각 owning Worker에서만 순차적으로 변경된다.
- DB 대기 중 다른 Actor가 진행하고 tick overrun, shard 편향, queue wait과 end-to-end
  p99를 수집한다.

콘텐츠 부하 기준선 (완료):

- `snf_load_client --scenario zone`은 연결마다 고유 Player를 인증하고, 설정한
  `players_per_zone`에 따라 Zone에 입장한 뒤 한 번에 하나의 Move를 지속 전송한다. bootstrap과
  gameplay 요청 수·RTT를 분리해 인증/입장 비용이 이동 p99를 왜곡하지 않게 한다. response type,
  request ID, Zone, route epoch, Move 좌표와 가변 visible-player payload를 검증한다. Enter 좌표는
  reconnect 시 저장된 authoritative 위치일 수 있으므로 요청 좌표와 같다고 가정하지 않는다.
- Release, 200 connections, 8 Zones(Zone당 25명), 12초, 연결당 20 req/s에서 총
  48,000/48,000 응답, timeout·invalid·socket error 0, gameplay RTT p99 `3.705 ms`였다.
  actor queue overflow, outbound admission failure, tick drop/skip/overrun도 모두 0이었다.
- 같은 실행의 reactor turn p99는 `0.655 ms`, outbound hand-off p99는 `0.983 ms`, outbound queue
  high-water는 `191/4096`이었다. Worker별 queue wait p99는 `0.197/0.262 ms`, 최종 처리량은
  `24,942/24,954`로 균형이었다. Zone command/tick 실행 p99는 각각 `2.815 µs/0.511 µs`였다.
- Zone을 4개로 줄인 Debug 탐색에서는 Worker 처리량이 약 1:3으로 기울었고 8개로 늘리자 거의
  균등해졌다. 이는 ConnectionScope나 runtime 통합이 해결할 cross-runtime 병목이 아니라 적은 hot
  Actor의 고정 shard 배치 특성이다.
- 따라서 이 기준선은 4.5 `ConnectionScope` 착수를 지지하지 않는다. 다음 측정은 MySQL을 붙인
  playable DB queue/operation 지연, 더 큰 AOI 응답과 단일 hot Zone을 각각 분리해 수행한다.
  그때 reactor p99/overflow 또는 cross-runtime hand-off가 지배적이면 4.5를 승격하고, 특정 Worker
  queue wait/tick overrun만 악화되면 Zone 분할·배치 정책을 먼저 다룬다.

후속 MySQL·hot Zone 측정 (완료):

- 동일한 Release 200 connections, 8 Zones, 12초, 20 req/s에서 MySQL adapter는
  47,800/47,800 응답, gameplay RTT p99 `10.654 ms`였고 timeout·invalid·overflow는 0이었다.
  같은 Docker 실행환경에서 바로 측정한 in-memory 대조군은 48,000/48,000, p99 `10.209 ms`였다.
  이전 실행의 3.705ms와 달라진 절대값보다 동시점 대조를 사용하며, 이 결과에는 MySQL 때문에
  steady-state gameplay p99가 유의하게 악화됐다는 증거가 없다.
- MySQL 실행의 reactor p99는 `0.262 ms`, outbound hand-off p99는 `0.786 ms`였다. repository는
  로그인·종료의 load/save 400건을 실패·거부 없이 처리했고 operation p99 `4.719 ms`, max
  `19.287 ms`였다. 시작 burst에서 두 DB Worker 앞 queue high-water가 `198/4096`까지 올랐으므로
  더 큰 login storm에서는 DB Worker/connection 수와 admission 정책을 별도로 측정해야 한다.
- 200명을 한 Zone에 모은 in-memory 측정도 48,000/48,000, gameplay p99 `8.960 ms`, overflow와
  tick overrun 0이었다. Zone command/tick p99는 `7.167 µs/0.959 µs`, owning Worker queue wait
  p99는 `1.049 ms`였다. 한 Worker가 48,327건, 다른 Worker가 94건을 처리해 고정 shard 편향은
  분명하지만 현재 부하에서는 deadline이나 용량 실패로 이어지지 않았다.
- 결론은 4.5를 계속 보류하는 것이다. 다음 성능 작업은 동시 login 규모를 단계적으로 올려 DB queue
  포화점을 찾거나, entity/AOI와 이동률을 올려 hot Zone의 tick/queue budget을 실제로 넘기는
  실험이다. 그 전에는 ConnectionScope, Zone 분할과 이동 coalescing을 구현하지 않는다.

#### 5.3 세부 기준

- 일반화된 scheduler 위에 `ZoneActor`와 Zone binding을 구현해 PlayerActor와 동일한
  mailbox, fairness와 Worker affinity 규칙을 사용한다.
- domain `TimerService`를 도입한다. timer identity, 주입 가능한 Clock, cancel과 late completion을
  정의하고 Actor mailbox에 typed timer event를 게시한다. 이 service는 gameplay 시간 이벤트만
  소유하고 선택적 network deadline primitive와 분리한다.
- `ZoneSimulationTick`은 이동, 기본 충돌과 AOI를 하나의 결정적 순서로 유지한다. respawn 같은 장주기
  콘텐츠는 별도 `TimerEvent`로 분리하고 tick 주기와 독립적으로 설정한다.
- tick event도 일반 command와 같은 turn budget과 단일 실행 규칙을 따르며 owning Worker에서 순차
  처리한다.
- `RouteCoordinator`가 `SessionRoute`와 `route_epoch`의 authoritative owner가 된다.
- route 변경 protocol이 이전 destination 정지, 새 destination 활성화, 새 route 공개 순서를
  보장한다. epoch은 원자성 구현이 아니라 stale destination 검출 수단이다.
- `ActorRuntimeDrained`는 Player·Zone mailbox, timer event와 actor task가 모두 빈 후에만 참이 된다.
  전체 종료 판정은 `docs/runtime-lifecycle-contract.md`의 predicate 조합을 따른다.
- tick 실행 시간, overrun, Actor별 command queue wait와 shard 편향을 측정한다. 이 수치로
  worker 수, affinity, fairness와 선택적 Runtime 최적화 필요를 판단한다.

## 6. Transactional Gameplay Slice

멱등한 구매 흐름 하나로 영속성의 트랜잭션 보장을 검증한다.

### 6.1 Bounded idempotent transaction repository (완료)

- `PurchaseRequest`는 Player, 64-bit idempotency key와 Product ID를 value로 저장소에
  전달한다. 재화 차감, 상품 지급과 idempotency record 생성은 하나의 임계 구역에서
  commit되며, 재화 부족과 inventory counter overflow는 어느 상태도 부분 변경하지 않는다.
- `InMemoryPlayerRepository`의 mutex 임계 구역은 transaction 의미의 결정적 참조 구현이다.
  6.3의 MySQL adapter는 재화, inventory와 idempotency row를 같은 DB transaction에서 commit해
  동일한 원자성을 보존한다.
- 같은 key와 같은 product는 저장된 결과를 replay하고, 같은 key와 다른 product는
  `IdempotencyConflict`를 반환한다. replay된 성공/실패 outcome은 변하지 않지만,
  PlayerActor가 나중 변경을 과거 snapshot으로 되돌리지 않도록 절대 balance와 item count는
  현재 authoritative record 값을 반환한다.
- Player별 idempotency 기록은 설정된 상한을 갖는다. 증거를 eviction해 effectively-once
  보장을 깨는 대신 새 key를 명시적으로 `IdempotencyCapacityExceeded`로 거부한다.
  존재하지 않는 product와 conflict는 idempotency 용량을 소비하지 않는다.
- bounded repository queue가 포화되거나 종료됐으면 `Unavailable`을 completion으로
  반환하고 Logic Worker를 block하지 않는다. commit, replay와 reject 건수를 계측한다.

### 6.2 Player command와 wire retry (완료)

- `Purchase=11`은 8-byte idempotency key와 4-byte Product ID를 받는 typed Player command다.
  인증된 persistent Player route에서만 실행하며, 인증 전 요청은 provisional actor에
  배치하지 않고 protocol error로 거부한다.
- Player binding은 repository transaction completion을 Actor async operation으로 기다린다. 해당
  Player만 suspend되고 같은 Worker의 다른 Actor는 계속 실행한다. repository와 DB Worker는
  Actor object, coroutine handle이나 mutable state를 보유하지 않는다.
- `PurchaseResult=12`는 status, replay 표시, key, product, 현재 balance와 item count를
  big-endian으로 반환한다. repository queue 거부는 `Unavailable`로 응답하며 저장소
  snapshot이 아닌 0으로 Actor 상태를 덮어쓰지 않는다.
- 중복 key와 commit 성공 후 응답 미수신을 검증한다. TCP 통합 테스트는 두 번째
  구매를 보낸 직후 응답을 읽지 않고 연결을 닫은 뒤, passivation·save·reconnect·load
  후 같은 key를 재전송해 replay 응답과 한 번의 effect만 관측한다.
- at-least-once 재전달을 transaction과 idempotency record로 effectively-once application으로
  바꾸며 exactly-once delivery를 가정하지 않는다. commit, replay와 reject는 각각
  repository metric으로 집계한다.

### 6.3 Durable DB adapter와 crash recovery (완료)

- `MySqlPlayerRepository`는 기존 `PlayerRepository` 경계 뒤의 bounded non-blocking FIFO와 전용
  Worker Pool을 유지하고, Worker마다 blocking MySQL connection을 소유한다. queue 포화는 호출 thread를
  막지 않고 기존 `Unavailable` 의미로 반환하며, operation failure와 latency 분포를 노출한다.
- MySQL 8/InnoDB schema version 1은 Player snapshot과 `(PlayerId, idempotency key)` unique row를
  저장한다. 구매는 Player row를 잠근 뒤 debit, grant와 성공·실패 idempotency 증거를 한 transaction으로
  commit한다. 같은 key 경합은 하나의 신규 적용과 하나의 replay로 직렬화된다.
- 서버는 기본적으로 결정적 in-memory adapter를 유지하고 `SNF_MYSQL_HOST`, `SNF_MYSQL_USER`와 선택적
  port/password/database 환경 변수가 주어지면 durable adapter를 선택한다. 실제 TCP 서버를 두 번
  기동한 통합 테스트가 마지막 Zone 위치를 포함한 Player 상태 복원을 검증한다.
- 별도 MySQL 통합 테스트는 repository 재생성, 두 adapter instance의 unique key 경합, key conflict,
  durable 실패 outcome과 bounded queue를 검증한다. `fork()` fault injection으로 commit 직전 crash는
  rollback 뒤 신규 적용되고, commit 직후 completion 전 crash는 retry에서 replay됨을 확인한다.
- idempotency 증거는 설정 상한에 도달하면 새 key를 거부하며 자동 eviction하지 않는다. `created_at`은
  운영 archive를 위한 관측점일 뿐이다. 보존 기간·partition/archive 정책과 그에 따른 retry 보장 창은
  실제 운영 요구가 정해질 때 결정해야 하며, 증거 삭제를 무제한 retry와 함께 약속하지 않는다.

## 7. Shared Content와 Projection

### 7.1 Party shared Actor slice (완료)

- `ActorKind::Party`와 typed `PartyActorBinding`/ingress를 같은 Actor-Bound Logic Runtime에
  등록한다. PartyId가 같은 join/leave는 하나의 owning Worker와 FIFO mailbox에서만
  membership을 변경하며, Player·Zone과 숫자 ID가 같아도 다른 Actor slot이다.
- `PartyCoordinator`는 connection·Player의 현재 Party route와 단조 membership epoch을 소유한다.
  하나의 session은 하나의 Party에만 속하며, post가 거부되면 새 route를 rollback한다.
- `PARTY_JOIN=13`, `PARTY_JOINED=14`, `PARTY_LEAVE=15`, `PARTY_LEFT=16`은 인증된
  Player에게만 허용한다. result는 status, PartyId, membership epoch와 PlayerId 오름차순
  member snapshot을 반환한다.
- Party 용량은 coordinator와 Actor 모두에서 상한을 갖는다. 상한에 닿은 join은
  route를 공개하지 않은 capacity probe로 같은 Party mailbox에 도착해 typed `PartyFull`을
  응답하고, protocol error로 연결을 닫지 않는다.
- 명시적 leave와 connection close는 membership epoch이 일치하는 leave를 Player passivation
  앞에 게시한다. coordinator route는 게시 즉시 삭제하지 않고 `leaving`으로
  유지해, Actor 결과 전에 다른 Party join이 공개되지 않게 한다. 마지막 member가
  나가면 `PassivateIfIdle`이 이미 수락된 mailbox tail을 폐기하지 않고 빈 Party를
  회수한다.
- TCP 통합 테스트는 두 Player가 같은 Party의 공유 member snapshot을 보는지, 세 번째
  Player가 `PartyFull` 후에도 PING을 주고받는지, 마지막 leave 후 Party가 passivate되는지
  검증한다. command, reject와 passivation request를 계측한다.

### 7.2 Projection slice (완료: in-memory reference)

- 랭킹 점수는 소유 `PlayerActor`만 변경한다. client wire에는 점수 증가 명령을 공개하지 않고,
  신뢰된 gameplay input인 `AwardRankingScoreCommand`가 절대 score와 Player별 단조 sequence를 담은
  `PlayerScoreChanged` domain event를 발행한다. score와 마지막 event sequence는 Player snapshot에도
  함께 저장된다.
- `ProtocolPlayerEffectSink`는 response와 domain event를 구분해, event-only result에는 유효한
  0-slot outbound reservation을 사용한다. event publish의 conflict, 순서 오류나 bounded log 포화는
  성공으로 숨기지 않고 Logic Runtime 실패로 승격한다.
- `InMemoryPlayerEventLog`는 `(PlayerId, sequence)` identity, 같은 값의 duplicate, 다른 값의
  conflict, Player별 연속 sequence와 전체 event 수 상한을 강제한다. 거부된 identity는 내부 map을
  생성하지 않아 잘못된 입력으로 메모리 상한을 우회할 수 없다.
- 별도 `RankingProjection`은 global offset을 연속 적용하고 score 내림차순, 동점 PlayerId
  오름차순으로 결정적 standings를 만든다. checkpoint restore 뒤 tail replay가 live view와
  동일한지, 같은 tail의 재실행이 idempotent한지 자동화 테스트로 검증한다.
- in-memory projection은 의미 참조 구현으로 남고, production `GameServer`는 7.3b의 durable checkpoint와
  outbox tail을 startup/live/shutdown 경로에서 소비한다. 여러 프로세스 projector의 leader election과
  season archive/retention은 아직 지원하지 않는다.

### 7.3 Durable ranking outbox와 checkpoint (완료)

- 상세 계약은 `docs/durable-ranking-contract.md`를 단일 기준으로 사용한다. Player score·sequence와
  outbox event를 MySQL transaction 하나로 commit하고, Actor는 authoritative completion만 적용한다.
- trusted award는 `(PlayerId, award_id)` durable identity를 가져야 한다. 같은 identity와 delta는
  replay하고 다른 delta는 conflict다. process-local request ID를 idempotency key로 사용하지 않는다.
- MySQL `AUTO_INCREMENT` gap은 현재 strict projection offset과 맞지 않으므로 transaction 안에서 잠그는
  stream cursor로 연속 offset을 할당한다. 병목이 측정될 때만 partition/gap 계약을 재검토한다.
- 구현은 7.3a award/outbox transaction, 7.3b ordered projector/checkpoint, 7.3c 부하·retention 결정으로
  나눈다. cross-zone handoff는 이 correctness gap을 닫은 뒤의 다음 콘텐츠 단계다.

#### 7.3a Durable award/outbox transaction (완료)

- `AwardRankingScoreCommand`는 non-zero `RankingAwardId`를 가지며, repository completion 전에는
  PlayerActor score를 변경하지 않는다. 동일 `(PlayerId, award_id)`·delta는 replay하고 다른 delta는
  conflict다. 오래된 replay는 event 원본과 현재 authoritative Player snapshot을 분리해 이후 score를
  과거 값으로 되돌리지 않는다.
- MySQL schema version 2는 `snf_event_stream`과 `snf_player_events`를 추가한다. Player score/sequence,
  outbox row와 strict global offset cursor는 한 InnoDB transaction에서 commit된다. rollback gap이 생기는
  `AUTO_INCREMENT` 대신 transaction row lock 아래 `last_offset + 1`을 할당한다.
- in-memory reference와 bounded Threaded adapter도 같은 request/result와 global event capacity를
  구현한다. award commit/replay/reject metric을 공통 repository snapshot에 추가했다.
- 실제 MySQL 통합 테스트는 두 repository instance의 동일 award 경합, 다른 delta conflict, 연속 global
  offset과 event tail을 검증한다. process crash fault injection에서 commit 직전은 rollback 후 신규
  적용되고 commit 직후 completion 전 crash는 같은 identity retry에서 replay된다.
- Debug 반복 5회, ASan/UBSan과 TSan의 실제 MySQL 경로가 통과했다.

#### 7.3b Ordered projector와 durable checkpoint (완료)

- 전용 blocking projector thread가 durable checkpoint를 복원하고 strict global offset tail을 batch로
  적용한다. construction의 동기 catch-up이 끝난 뒤에만 `GameServer`가 gameplay ingress를 시작한다.
- 실행 중 bounded polling은 read/checkpoint 실패를 별도 metric으로 남기고 재시도한다. committed tail,
  projection offset/lag와 checkpoint offset을 server snapshot에 노출한다.
- 정상 종료는 Actor/repository transaction drain 뒤 projector를 stop해 마지막 tail replay와 checkpoint를
  시도한다. checkpoint 실패는 live projection이나 outbox를 되돌리지 않으며 다음 startup replay가
  복구한다.
- in-memory restart와 poll failure 재시도, 실제 MySQL checkpoint/tail restart, checkpoint commit 직전·직후
  process crash, `GameServer` 두 번 재시작을 자동화 테스트로 검증한다.
- Debug 실제 MySQL 경로 5회 반복과 ASan/UBSan·TSan 전체/실제 MySQL 경로가 통과했다.

#### 7.3c 운영 부하와 retention 결정 (완료)

- `snf_ranking_benchmark`가 Player/award/Worker/in-flight/checkpoint 주기를 바꿔 throughput, end-to-end와
  repository award 지연, queue high-water, projector poll/checkpoint 지연과 최종 lag을 출력한다.
- MySQL 8.0.46 local Docker Release 기준 100 Player·2,000 award에서 1/2/4 Worker 처리량은 각각
  `990/1,149/1,092 awards/s`, repository award p99는 `1.704/3.670/5.767 ms`였다. strict cursor가
  처리량 상한이며 Worker 4개는 scale-out하지 않고 lock wait만 늘렸다. 기본 Worker 2개를 유지한다.
- 지속 부하에서 unbounded catch-up이 checkpoint를 종료까지 미루던 문제를 측정으로 발견했다. live poll은
  회차당 `batch_size × max_batches_per_poll`로 제한하고 batch 사이 checkpoint를 시도한다. 5,000 events,
  checkpoint 주기 1,000에서 write 5회와 final lag 0을 확인했다.
- v3 full replacement는 5,000 Player checkpoint p99 `156.429 ms`, award max `123.479 ms`였다. schema
  v4는 새 generation을 먼저 쓴 뒤 stream cursor와 meta pointer만 원자 교체한다. checkpoint 자체 p99는
  `182.917 ms`지만 award max는 `71.542 ms`, 처리량은 `1,036→1,071 awards/s`로 개선됐고 snapshot 작성
  중 별도 award가 완료되는 통합 테스트도 추가했다.
- 현재 trusted award producer에는 이 기준선을 넘는 요구율이 정의되지 않았으므로 strict global cursor를
  유지한다. 배포 환경의 지속 목표가 해당 환경 포화 처리량의 50%를 넘거나 award p99 예산을 위반할
  때만 partition/gap 계약을 다시 연다.
- `(PlayerId, award_id)` replay 증거가 outbox row와 함께 있으므로 시간 기반 자동 prune은 같은 award의
  이중 적용을 허용한다. 현재 single-season schema에서는 outbox 전체를 보존한다. season identity,
  receipt tombstone, final checkpoint와 검증된 archive/backup을 함께 설계한 뒤에만 이전 season을 prune한다.
  상세 조건과 원자료는 `docs/ranking-performance-baseline.md`에 기록한다.
- schema v4를 포함한 실제 MySQL 경로 Debug 5회 반복과 ASan/UBSan·TSan 전체 테스트가 통과했다.

### 7.4 Cross-Zone handoff (완료)

- 상세 계약은 `docs/cross-zone-handoff-contract.md`를 단일 기준으로 사용한다. route epoch만으로 전환을
  원자화하지 않고 reactor 소유 `Stable/Transferring` state machine이 source drain → target activation →
  route publish 순서를 직렬화한다.
- Worker completion은 reserved bounded value channel로 reactor에 돌아온다. handoff 하나당 단계 하나만
  in-flight라 completion slot을 admission에서 예약할 수 있고, outbound action과 별도 drain predicate를
  가진다.
- client command token은 reactor transition record 하나가 소유하고 내부 source/target/보상 command는
  client credit을 만들지 않는다. 성공 응답, typed failure 또는 연결 종료 중 하나로 한 번 종결한다.
- 구현은 7.4a transition/completion channel, 7.4b happy path/protocol, 7.4c compensation·disconnect·shutdown
  순서로 나눈다.

#### 7.4a Transition state와 bounded completion (완료)

- reactor 소유 `RouteCoordinator`에 connection별 `ZoneHandoffId`와
  `LeaveSource → EnterTarget → RestoreSource` 전환 상태를 추가했다. 전환 중에는 stable route 조회를
  숨기되 source와 target Zone의 수명 회계는 모두 유지한다. target 성공과 source 복구는 각각 단조 증가한
  새 route epoch을 공개한다.
- `ZoneTransitionChannel`은 `max_zone_handoffs`와 같은 고정 용량을 갖고 handoff 시작 시 ticket 하나를
  예약한다. 단계는 한 번에 하나만 in-flight이므로 Worker publish는 이미 확보된 value slot을 사용하며,
  같은 ticket의 중복 publish와 다른 handoff identity를 거부한다. release가 queued completion과 경합하면
  실제 consume 뒤 reservation을 반환한다.
- 최소 capacity에서 전환 admission 거부, source leave 전 rollback, source restore, target publish,
  abandon과 stale identity를 검증했다. completion channel은 여러 Worker의 동시 publish, cancel, slot 재사용과
  queued release를 검증한다. 실제 Worker→reactor 연결과 TCP happy path는 7.4b 범위다.

#### 7.4b Happy path와 protocol (완료)

- client의 cross-zone `EnterZone`은 reactor transition record가 terminal token 하나를 소유하고, source
  Leave와 target Enter는 reply/credit 없는 internal command로 같은 Actor mailbox에 게시한다. Worker는
  immutable handoff context를 reserved channel로 반환하고 reactor는 회차당 설정된 completion 수만 처리한다.
- source Leave completion 전에는 stable route 조회를 숨기고, target Enter가 `Applied` 또는
  `AlreadyPresent`로 끝난 뒤에만 target epoch을 공개한다. 그때 session location을 authoritative target
  좌표로 바꾸고 source timer를 취소한 뒤 `ZoneEntered` 하나와 client terminal 하나를 방출한다.
- 전환 중 Enter/Move/Leave는 source나 target mailbox에 게시하지 않고 각 message type의
  `TransitionInProgress` 응답으로 끝낸다. target timer·handoff reservation·source command admission이 source
  Leave 전에 실패하면 source stable route를 유지하고 `TransferFailed`를 응답한다.
- 단위 테스트는 source completion 전 route 비공개, typed busy, target publish, source post `Full` rollback과
  terminal/reservation 회계를 검증한다. 실제 TCP는 Zone A enter → Zone B handoff → target Move/Leave를 새
  epoch과 좌표로 왕복하고 completion 두 개와 transition duration을 계측한다.

#### 7.4c Compensation, disconnect와 shutdown (완료)

- source Leave 뒤 target post/result가 실패하면 target epoch보다 큰 restore epoch으로 source Enter를
  게시한다. restore completion 뒤에만 source route와 location을 다시 공개하고 `TransferFailed`를 한 번
  응답한다. restore admission/result도 실패하면 route를 추측하지 않고 location을 authoritative `none`으로
  만든 뒤 기존 outbound admission-failure 경로로 연결 종료를 요청한다.
- transition 중 `ConnectionClosed`는 즉시 Player save를 게시하지 않고 기존 bounded lifecycle retry
  deque에 남는다. source가 아직 떠나는 중이면 그 completion 뒤 none으로 끝내고, target 또는 복구 source가
  적용된 뒤라면 내부 cleanup Leave completion까지 기다린다. active transition이 사라진 다음 같은 close
  value가 재시도되어 authoritative none snapshot으로 Player close/save를 진행한다.
- shutdown은 Logic Runtime drain만으로 끝나지 않고 active handoff와 reserved completion channel이 모두 빈
  조건을 추가로 관찰한다. completion이 budget보다 많으면 shared eventfd를 다시 깨워 다음 reactor turn에서
  계속 처리하고, grace deadline의 cancel은 route/token/reservation을 명시적으로 회수한다.
- 단위 테스트는 target post `Full` 뒤 source restore, restore도 `Full`인 fatal close, stale completion,
  source leave 전 disconnect, target 적용 뒤 cleanup과 shutdown cancel을 검증한다. TCP reactor 테스트는
  Logic drain 뒤 control state가 남은 동안 shutdown이 반환하지 않는지 확인한다. Debug, ASan·UBSan과 TSan
  전체 회귀를 완료 기준으로 사용한다.

## 선택적 인프라 트랙: io_uring Network Backend

io_uring은 콘텐츠 단계의 선행 조건이 아니다. epoll backend와 첫 영속성 slice로
`FrameIngress`, `OutboundSink`, lifecycle 계약을 검증한 뒤 진행한다.

- `NetworkRuntime` lifecycle 계약을 두 번째 backend가 실제로 필요로 하는 최소 범위에서 추출한다.
- `IoUringNetworkRuntime`은 동일한 `FrameEnvelope`를 제출하고 `OutboundAction`을 소비한다.
- operation/buffer lifetime, cancel과 stale completion을 검증한다.
- epoll과 동일한 기능·안정성·부하 테스트를 적용해 결과를 비교한다.

## 공통 원칙

- 게임 상태는 소유 Actor만 수정한다. network 경로의 task는 어떤 실행 pool에서 실행되든 게임 상태를
  직접 수정하지 않는다. 향후 실행 pool 통합을 선택해도 이 소유권 규칙을 완화하지 않는다.
- Handler는 물리 thread가 아니라 상태 소유 PlayerActor, ZoneActor 또는 공유 콘텐츠 Actor를 선택한다.
- 다른 Runtime의 mutable 객체나 coroutine handle을 직접 참조하지 않는다.
- 모든 비동기 queue와 in-flight operation에는 명시적인 상한이 있다.
- command별 순서, 멱등성, 포화와 전달 보장 정책을 정의한다.
- Service Layer, lock-free, microservice와 Actor 내부 병렬화는 실제 필요와 측정 전에는 도입하지
  않는다.
