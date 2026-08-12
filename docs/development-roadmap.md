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
→ ConnectionScope
→ UnifiedRuntime 통합
→ Auth/Persistence Vertical Slice
→ Minimal ZoneActor와 TimerService
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
- inbound command, outbound action, lifecycle, post result 헤더를 분리했다. Phase 3.8 이후
  completion identity는 단일 `RuntimeId::Logic`이며 `ActorRuntimeConfig`에 별도로 지정하지 않는다.
- reactor가 관찰한 `ConnectionClosed{connection, cause}`는 기존 worker ingress FIFO의 command 뒤에
  게시된다. actor가 없는 close는 슬롯을 만들지 않으며, close를 소비한 owning worker만 actor를
  evict한다.
- lifecycle post가 포화되면 reactor 소유 pending deque가 회차당 최대 64건을 재시도한다. active
  session과 pending close는 하나의 bounded lifecycle slot 예산을 공유하며, 예산 소진 시 새 연결을
  Actor 생성 전에 거부한다. command overload 통계와 lifecycle 통계는 분리하며, shutdown에서는 닫힌
  ingress에 close를 재주입하지 않는다.

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
  command 수의 상한이다. 연결 수명에 속하며 Actor 활성화와는 별개다.
- **command terminal**: handler가 반환한 시점이 아니라 필요한 effect가 적용되고 성공, 실패 또는 취소
  중 하나의 최종 결과가 확정된 시점이다. 이때 credit 점유가 끝난다. 응답 effect가 있는 command와 없는
  command에 같은 정의를 적용한다. 신호의 생산 경로는 4.1에서, credit 반환은 4.5에서 구현한다.

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
- baseline은 `snf_load_client` 부하 실행 중의 주기 보고와 종료 요약으로 수집한다. 4.6 통합 전후 비교
  대상은 reactor turn 지연, Actor queue wait, outbound hand-off 시간과 outbound queue depth다.

blocking outbound는 4.6 UnifiedRuntime 통합의 blocker였다. 통합 후에는 outbound를 비우는 주체와
대기하는 주체가 같은 pool에 있으므로, blocking 대기를 남겨두면 pool 전체가 진행하지 못한다. 단계
4.1에서 제거했다.

완료 기준:

- 현재 포화 동작과 목표 포화 동작이 아키텍처 초안 §8에 현재/목표/예정 단계로 기록된다.
- in-flight credit과 command terminal 정의가 문서에 고정되고 4.1과 4.5가 이를 참조한다.
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
  아무 신호도 내지 않는다. 4.5의 credit 취득도 같은 경계여야 두 수치가 일치한다.

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
하지 않았다. 4.6이 이 경로를 다시 구성한다.

## 4.5. ConnectionScope

연결 하나의 네트워크 상태와 수명을 장수명 coroutine 쌍으로 표현한다. 게임 상태는 여전히 Actor가
소유하고 `ConnectionScope`는 네트워크 상태만 소유한다.

현재 `TcpServer::run()`은 coroutine을 재개하는 executor가 아니라 동기 epoll 루프다. 따라서 이 단계의
산출물에는 최소 Executor가 포함된다.

- 기존 reactor loop에 최소 Executor adapter와 ready queue를 추가한다.
- I/O readiness는 coroutine을 inline resume하지 않고 ready queue에 게시한다.
- async read/write awaiter와 runtime deadline primitive를 제공한다.
- 새 thread pool은 만들지 않으며 기존 reactor thread가 executor를 실행한다.
- 이 Executor 계약과 ready queue는 4.6 UnifiedRuntime에서 그대로 재사용한다.

그 위에 다음을 올린다.

- read loop과 write loop을 분리하고 Session 상태를 소유권으로 나눈다. read loop은 recv buffer, frame
  assembler와 in-flight credit을, write loop은 send buffer와 pending send 상태를 소유한다. 공유하는
  것은 종료 상태 하나로 줄인다.
- credit이 소진되면 socket 읽기를 중지하고, 4.1의 command terminal 신호로 credit을 반환해 재개한다.
  4.1은 그 신호를 command를 승인하는 binding 경계에서 생산하므로 credit 취득도 같은 경계여야 한다.
  protocol 단계에서 거부되는 frame은 command가 된 적이 없으므로 credit을 소비하지 않는다.
- read loop, write loop, deadline과 control이 모두 `requestClose(cause)`를 호출할 수 있고,
  `ConnectionScope`만 단일 terminal 전이를 수행해 `ConnectionClosed`를 정확히 한 번 게시한다.
- `ConnectionScope`는 새 read admission을 중단하고, read loop이 진행 중인 ingress 게시를 마친 뒤 같은
  ordered ingress에 `ConnectionClosed`를 게시한다. 따라서 terminal 전이 전에 승인된 그 연결의 command
  보다 뒤에 위치한다.
- heartbeat와 idle timeout은 runtime deadline primitive로 표현하며 Phase 6의 domain `TimerService`에
  의존하지 않는다.

착수 직전 `docs/connection-scope-contract.md`에 Executor 계약, 상태 소유권, 단일 종결, close 순서와
취소 전파를 고정한다.

완료 기준:

- inbound frame을 조용히 드롭하지 않고 credit 소진 전에 읽기를 중지한다.
- 종료 원인을 누가 먼저 관측하든 `ConnectionClosed`가 정확히 한 번, terminal 전이 전에 승인된 command
  뒤에 게시된다.
- coroutine frame 파괴, socket close와 deadline 만료가 경합해도 use-after-free가 없다.
- partial send와 `EPOLLOUT` 재무장이 write loop 안에서 처리된다.
- Debug, ASan·UBSan, TSan에서 연결 폭주와 강제 종료 시나리오가 통과한다.

## 4.6. UnifiedRuntime 통합

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
- blocking DB adapter를 bounded DB Worker Pool에 연결하고 player load/save가 Actor coroutine을
  suspend하도록 구현한다. DB queue와 connection 수에는 명시적인 상한과 포화 정책을 둔다.
- DB Worker는 coroutine이나 Actor 포인터를 보유하지 않는다.

성장·업적·기간제 이벤트는 이 slice 이후 Player module로 추가한다. 랭킹과 시즌 정산은
Shared Content 또는 projection 단계로 미룬다.

## 6. 최소 ZoneActor와 TimerService

- 일반화된 scheduler 위에 `ZoneActor`와 Zone binding을 구현해 PlayerActor와 동일한
  mailbox, fairness와 Worker affinity 규칙을 사용한다.
- domain `TimerService`를 도입한다. timer identity, 주입 가능한 Clock, cancel과 late completion을
  정의하고 Actor mailbox에 typed timer event를 게시한다. 이는 4.5의 runtime deadline primitive와 다른
  층이다. deadline primitive는 I/O await timeout과 취소를 담당하고, `TimerService`는 gameplay 시간
  이벤트를 담당한다.
- `ZoneSimulationTick`은 이동, 기본 충돌과 AOI를 하나의 결정적 순서로 유지한다. respawn 같은 장주기
  콘텐츠는 별도 `TimerEvent`로 분리하고 tick 주기와 독립적으로 설정한다.
- tick event도 일반 command와 같은 turn budget과 단일 실행 규칙을 따르며 owning Worker에서 순차
  처리한다.
- `RouteCoordinator`가 `SessionRoute`와 `route_epoch`의 authoritative owner가 된다.
- route 변경 protocol이 이전 destination 정지, 새 destination 활성화, 새 route 공개 순서를
  보장한다. epoch은 원자성 구현이 아니라 stale destination 검출 수단이다.
- `ActorRuntimeDrained`는 Player·Zone mailbox, timer event와 actor task가 모두 빈 후에만 참이 된다.
  전체 종료 판정은 `docs/runtime-lifecycle-contract.md`의 predicate 조합을 따른다.
- tick 실행 시간, overrun, Actor별 command queue wait와 shard 편향을 측정한다. 이 수치로 4.6 통합 후의
  worker 수, affinity와 fairness를 조정한다.

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

- 게임 상태는 소유 Actor만 수정한다. network 경로의 task는 어떤 실행 pool에서 실행되든 게임 상태를
  직접 수정하지 않는다. 4.6 통합은 실행 pool을 합치는 것이며 이 소유권 규칙을 완화하지 않는다.
- Handler는 물리 thread가 아니라 상태 소유 PlayerActor, ZoneActor 또는 공유 콘텐츠 Actor를 선택한다.
- 다른 Runtime의 mutable 객체나 coroutine handle을 직접 참조하지 않는다.
- 모든 비동기 queue와 in-flight operation에는 명시적인 상한이 있다.
- command별 순서, 멱등성, 포화와 전달 보장 정책을 정의한다.
- Service Layer, lock-free, microservice와 Actor 내부 병렬화는 실제 필요와 측정 전에는 도입하지
  않는다.
