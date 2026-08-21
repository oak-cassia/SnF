# Player 상태 소유권과 persistence 계약

## 1. 원칙

Live Server가 실행 중 Player gameplay state의 authority다. DB는 snapshot 저장과 login/reconnect
복구를 담당하며 정상 command를 다시 판정하지 않는다.

```text
PlayerActor
├── Session
│   ├── identity
│   ├── handled command count
│   └── last Zone location
└── Economy
    ├── currency balance
    ├── purchased item count
    └── bounded purchase evidence
```

Session과 Economy는 별도 Actor가 아니라 PlayerActor 내부 구성 단위다. 잔액 차감과 상품 지급처럼
하나의 불변식으로 변경되는 값은 같은 mailbox에서 처리한다.

## 2. Command 규칙

- PlayerActor만 PlayerState를 수정한다.
- 다른 thread는 const reference를 읽지 않고 command 또는 immutable snapshot을 사용한다.
- `restore()`는 login/복구 시 owning Worker에서만 실행한다.
- `PurchaseCommand`는 상품 정의, 잔액, 지급량과 idempotency를 한 turn에서 판정한다.
- 같은 key와 product는 저장된 outcome을 replay한다.
- 같은 key와 다른 product는 `IdempotencyConflict`다.
- evidence 상한에 도달하면 기존 증거를 지우지 않고 새 key를 거부한다.

현재 evidence의 수명은 Actor activation과 같다. process crash 또는 passivation 뒤 같은 key가 다시
오면 신규 command로 처리될 수 있다. 이 범위를 넘어서는 멱등성이 필요하면 별도 durable 요구사항을
정의해야 한다.

## 3. Dirty snapshot

성공한 Economy 변경은 Economy dirty bit을 설정한다. owning Worker는 flat `PlayerRecord` snapshot을
만들어 `PlayerPersistenceService`의 bounded queue에 non-blocking으로 제출한다.

- admission 성공: 제출 시점의 dirty bit을 지운다.
- admission 실패: dirty bit을 복원해 다음 command에서 재시도한다.
- DB completion은 PlayerActor state를 다시 덮어쓰지 않는다.
- location과 economy는 같은 authoritative snapshot으로 저장한다.

## 4. PlayerPersistenceService

Service는 production에서 Player snapshot을 저장하는 유일한 경로다.

- 같은 Player의 pending snapshot은 최신 값으로 coalesce한다.
- 같은 Player save는 동시에 두 개 실행하지 않는다.
- 다른 Player save는 repository Worker에서 병렬 실행될 수 있다.
- background save 실패는 snapshot을 유지하고 retry한다.
- logout final save는 이전 background save 뒤에 직렬화한다.
- shutdown은 accepted snapshot과 final request가 terminal 결과에 도달할 때까지 flush한다.

## 5. Repository

```cpp
asyncLoad(PlayerId, PlayerLoadCompletion)
asyncSave(PlayerRecord, PlayerSaveCompletion)
```

Repository는 Actor, ActorState, mutable state 또는 coroutine handle을 받지 않는다. in-memory adapter는
결정적 기본 실행에 사용하고, MySQL adapter는 bounded queue와 전용 Worker Pool 뒤에서 blocking C API를
실행한다.

## 6. Durability 한계

현재 구매 성공은 snapshot 저장 완료 전에 응답된다. 따라서 flush 전 process crash에서는 최근 economy
변경이 사라질 수 있다. 이는 무료/게임 내 NPC 상품을 가정한 명시적 정책이지 durable transaction과
동일한 보장이 아니다.

durable reward나 결제가 필요해지면 기존 handler에 boolean 옵션을 추가하지 않는다. 요구되는 atomicity,
retry window와 authority를 먼저 정의하고 별도 vertical slice로 구현한다.

## 7. 검증

- 구매 성공/잔액 부족/없는 상품/inventory overflow
- 같은 key replay와 다른 product conflict
- evidence capacity
- snapshot queue rejection 시 dirty 복원
- Player별 coalescing과 non-overlap
- background retry와 final save ordering
- disconnect/save/reconnect 복원
- shutdown final flush
- Debug, TCP integration과 TSan
