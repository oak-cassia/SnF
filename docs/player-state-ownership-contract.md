# Player 상태 소유권과 persistence 계약

> 문서 상태: **domain authority 보존 / persistence 실행 경로 전환 예정**
> Player gameplay state와 durability 의미는 이 문서가 소유한다. thread, queue, coroutine과 DB 진행
> 방식은 [Unified Worker Runtime](./architecture/unified-worker-runtime.md)이 우선한다. 현행
> `PlayerPersistenceService`와 blocking MySQL worker는 target 구조가 아니다.

## 1. Authority

Live server가 실행 중 Player gameplay state의 authority다. DB는 snapshot 저장과 login/reconnect 복구를
담당하며 정상 command를 다시 판정하지 않는다.

```text
PlayerActor
├─ Session
│  ├─ identity
│  ├─ handled command count
│  └─ last Zone location
├─ Economy
│  ├─ currency balance
│  ├─ purchased item count
│  └─ bounded purchase evidence
└─ Skills
   └─ sorted owned skill IDs + equipped skill ID
```

Session, Economy와 Skills는 별도 Actor가 아니다. 잔액 차감과 상품 지급처럼 하나의 불변식으로 바뀌는
값은 같은 Actor turn에서 처리한다.

## 2. Command 규칙

- PlayerActor만 Player gameplay state를 수정한다.
- 다른 Worker는 const reference나 pointer를 읽지 않고 message 또는 immutable snapshot을 사용한다.
- `restore()`는 activation load를 소유한 Worker에서만 실행한다.
- `PurchaseCommand`는 상품 정의, 잔액, 지급량과 idempotency를 한 turn에서 판정한다.
- 스킬 상품은 이미 보유했는지를 잔액보다 먼저 판정하고 성공할 때만 잔액과 보유 목록을 함께 확정한다.
- `EquipSkillCommand`는 알려진 보유 스킬만 장착하며 Room 입장 뒤 변경은 다음 입장부터 적용한다.
- 같은 key와 product는 저장된 outcome을 replay하고 같은 key와 다른 product는
  `IdempotencyConflict`로 끝낸다.
- evidence 상한에 도달하면 기존 증거를 지우지 않고 새 key를 거부한다.

현재 evidence의 수명은 Actor activation과 같다. process crash 또는 향후 passivation 뒤 같은 key가
다시 오면 새 command로 처리될 수 있다. 이 범위를 넘는 멱등성은 별도 durable 요구사항이다.

## 3. Connection lifecycle

Connection identity는 Player domain state가 아니다. connection owner Worker의 `ConnectionSlot.session`과
application routing state가 `ConnectionRef{id, generation, owner}`로 admission, persistent Player routing과
one-live-session을 판정한다. Actor owner가 다르면 concrete message로 전달한다.

- stale generation은 Player command admission 전에 거부한다.
- connection이 닫히면 session route는 final persistence와 Actor cleanup 정책이 끝날 때까지 Closing을
  유지해 이전 mailbox tail과 reconnect가 섞이지 않게 한다.
- disconnect 전에 수락된 FIFO command는 connection close 뒤 실행돼 Player state를 바꿀 수 있다. 이는
  이미 수락된 tail을 보존하는 정책이지 신규 command를 허용한다는 뜻이 아니다.
- cleanup은 Player identity와 `ConnectionRef`가 현재 Closing session과 모두 일치할 때만 route index를
  제거한다.
- 이전 activation이나 connection의 늦은 completion은 incarnation/generation 검증 실패로 state를
  변경하지 않는다.

구체 route/transition owner는 application workflow 전환 단계에서 확정하되, 별도 global Reactor가 모든
connection mutable state를 소유하는 구조로 되돌리지 않는다.

## 4. Snapshot 의미

성공한 Economy, Progression과 Skills 변경은 해당 dirty bit을 설정하고, 저장할 때는 전체
`PlayerRecord` immutable snapshot을 만든다.

- location, economy, progression과 skill loadout은 하나의 authoritative snapshot으로 저장한다.
- snapshot은 저장 시점의 Player state를 복사한 값이며 DB/backend가 Actor state를 다시 읽지 않는다.
- DB completion은 Player gameplay state를 덮어쓰지 않는다.
- mutation save의 자동 retry는 금지한다. retry하려면 overwrite가 안전하다는 계약이나 idempotency key를
  명시한다.
- save admission 실패, timeout과 connection loss가 dirty state와 client outcome에 미치는 의미를 각
  use case가 명시해야 한다.

## 5. Target persistence 실행

기본 production 경로는 Player owner Worker의 poller에서 진행하는 Worker-local native async
`DbClient`다.

```text
Player command 또는 activation
-> immutable DbRequest 생성
-> DbClient.tryStart(request, AwaitKey)
-> Rejected / CompletedInline / Pending
-> Pending이면 ActorSlot.blocked = ActivationLoad | SuspendedDbCommand
-> DB readiness progress
-> Worker.completeDb(AwaitKey, DbResult)
-> Actor phase에서 activation 완료 또는 coroutine resume
```

`DbClient`는 DB connection과 wire/protocol 진행 상태만 소유한다. Actor pointer, coroutine handle,
Player state나 retry policy를 보관하지 않는다. 동기 MySQL API를 즉시 제거할 수 없을 때만 선택
`BlockingAdapterExecutor`를 사용하고, job queue와 completion slot을 submit 시점에 함께 예약한다.

Target core에는 다음을 두지 않는다.

- `PlayerPersistenceService`를 runtime 필수 계층으로 두는 구조
- 여러 Worker가 공유하는 blocking repository worker pool을 기본 DB 경로로 사용하는 구조
- Actor continuation/deadline을 담는 repository-side pending table
- 의미가 불명확한 `StartDetachedEffect`

## 6. Deferred durability 결정

현재 구매·장착 성공은 snapshot 저장 완료 전에 응답될 수 있다. flush 전 process crash에서 최근
economy나 skill loadout 변경이 사라질 수 있다는 명시적 정책이며 durable transaction과 같은 보장이
아니다.

새 runtime에서 이 정책을 구현할 때 background save를 generic detached effect로 숨기지 않는다. 각
use case는 다음 중 하나를 명시적으로 선택한다.

1. 현재 Player command가 DB save를 await하고 terminal outcome을 저장 결과 뒤에 낸다.
2. 독립 lifecycle이 필요하면 persistence Coordinator Actor가 immutable snapshot, Player별 ordering과
   retry state를 소유하고 자신의 DB command만 await한다.
3. process restart를 넘는 보장이 필요하면 durable ledger/outbox를 별도 subsystem으로 설계한다.

2번에서 PlayerActor는 snapshot message를 보낸 뒤 현재 turn을 끝내며 Coordinator의 mailbox 응답을
suspended coroutine으로 기다리지 않는다. Coordinator의 필요성은 deferred durability와 PlayerActor
응답성 요구로 증명해야 한다. 정확한 Player save 정책은 개발 로드맵 8단계의 승인 항목이다.

## 7. Battle reward

일반 battle reward는 다음 gameplay 책임 경계를 유지한다.

```text
grant tell 수락 + 최초 load 성공
-> PlayerActor가 reward 적용 책임을 인수
-> immutable snapshot 또는 explicit dirty/save state로 persistence 진행

grant tell 거절
-> 계측된 허용 유실

최초 load 실패
-> 계측된 허용 유실; reward를 적용하지 않음
```

Room은 terminal 정리 뒤 generic retry timer를 소유하지 않는다. 고가치 보상과 외부 결제는 durable
원장과 멱등 키가 필요하며, 기존 handler에 boolean 옵션을 추가하지 않고 atomicity, retry window와
authority를 별도 vertical slice로 설계한다.

## 8. 검증

- 구매 성공/잔액 부족/없는 상품/inventory overflow
- 스킬 구매/중복 보유/장착 성공·실패와 보유·장착 round trip
- 같은 key replay, 다른 product conflict와 evidence capacity
- activation load success/failure와 queued command 종결
- save admission rejection, timeout, late completion과 dirty-state 정책
- old incarnation/generation/operation completion의 safe drop
- disconnect/save/reconnect 복원과 exact-match cleanup
- 서로 다른 battle grant의 누적과 대상 Player가 다른 grant 거부
- grant tell 거절·최초 load 실패 계측
- native DB partial I/O와 slow-server conformance
- shutdown 중 DB completion/cancel race
- Debug, TCP/MySQL integration, ASan·UBSan과 TSan
