# 투사체 공격 스킬 구현 계약과 작업 계획

> 상태: 구현 전 계획 채택
> 첫 콘텐츠: `ArcaneBolt` 유도 투사체 공격
> 범위: Room 안에서의 투사체 생성·이동·충돌·관찰과, 이후 Player의 구매·장착·영속화 연결

## 1. 목표

현재 `Slash`는 `UseSkill` command를 처리하는 turn 안에서 범위 내 모든 적에게 즉시 피해를 준다.
`ArcaneBolt`는 cast와 피해 사이에 시간이 흐르는 첫 스킬이다.

```text
UseSkill(ArcaneBolt)
→ 서버가 target 선택
→ Room이 Projectile 생성
→ RoomSimulationTick마다 이동
→ target과 충돌
→ 피해 적용 후 Projectile 제거
```

이 작업의 목적은 스킬 수를 하나 늘리는 데 그치지 않는다.

- `std::variant`로 서로 다른 공격 전달 방식을 값 타입으로 표현한다.
- Room Actor가 수명이 짧은 전투 객체를 단독으로 소유한다.
- cast, 이동, 충돌과 제거의 결정적 순서를 정한다.
- 투사체 수와 digest event 수에 상한을 둔다.
- 서버 상태를 wire event로 노출해 실제 클라이언트에서 관찰한다.
- 완성된 투사체를 Player의 스킬 구매·장착·영속화 흐름에 연결한다.

기존 개발 로드맵에서 projectile은 Wave Battle의 비범위였다. 이 문서는 다음 콘텐츠로 **유도형 단일 대상
투사체 하나만** 다시 범위에 넣는다. 범용 물리 엔진이나 모든 투사체 형태를 함께 열지 않는다.

## 2. 첫 버전의 게임 규칙

### 2.1 스킬 수치

수치는 구현과 테스트를 시작하기 위한 초기값이다. 구조가 완성된 뒤 플레이 테스트로 조정한다.

| 스킬 | 전달 방식 | 공격력 | 사거리 | 쿨다운 |
| --- | --- | ---: | ---: | ---: |
| `Slash` | 시전자 중심 범위 즉시 공격 | 100% | 12 | 1초 |
| `ArcaneBolt` | 가장 가까운 적을 추적하는 단일 투사체 | 160% | target 획득 40 | 1.5초 |

`ArcaneBolt` 투사체의 초기값:

- Room tick마다 4 좌표 단위 이동
- target과 거리 1 이하가 되면 충돌
- 발사 뒤 3초가 지나면 만료
- Room 하나에 동시에 최대 128개
- splash damage, 관통, 치명타와 상태 효과 없음

### 2.2 target 선택

client는 `EnemyId`, 방향이나 좌표를 보내지 않는다. Room이 cast 시점에 target을 결정한다.

1. 현재 살아 있는 적만 후보로 둔다.
2. 시전자 위치에서 획득 사거리 안에 있는 적만 후보로 둔다.
3. 제곱 거리가 가장 작은 적을 선택한다.
4. 거리가 같으면 작은 `EnemyId`를 선택한다.
5. 후보가 없으면 `SkillWhiffed`를 만들고 투사체는 만들지 않는다.

target이 없는 cast도 유효한 cast다. 현재 `Slash`와 같이 request sequence와 cooldown을 소비한다.

### 2.3 추적과 충돌

- 투사체는 cast 때 선택한 target 하나만 추적한다.
- target이 움직이면 다음 tick에서 target의 새 위치를 향한다.
- target이 죽거나 Room에서 제거되면 다른 적으로 갈아타지 않고 `TargetLost`로 제거된다.
- 이동 전 이미 충돌 범위 안이면 이동 event 없이 바로 충돌한다.
- 그렇지 않으면 `moveToward()`로 한 번 이동하고, 이동 뒤 충돌 범위를 다시 확인한다.
- `moveToward()`가 target 좌표를 넘지 않으므로 첫 버전에는 선분 기반 swept collision을 추가하지 않는다.

이는 조준 방향으로 직진하는 projectile이 아니라 서버 자동 target 유도형이다. 직선형 투사체는 aim vector,
고정소수점 위치, arena 경계 이탈과 tunneling 판정을 별도 계약으로 정한 뒤 추가한다.

### 2.4 피해와 cooldown

- 피해량은 **cast 시점**의 Player 전투 스냅샷으로 한 번 계산해 Projectile에 저장한다.
- 충돌 시점에 PlayerActor나 SkillCatalog를 다시 조회하지 않는다.
- 충돌 피해는 `min(projectile.damage, target.health)`다.
- cooldown은 투사체 충돌이나 제거가 아니라 cast가 성공한 시점부터 시작한다.
- 같은 request sequence는 투사체를 두 번 만들지 못한다.

발사 시점 피해를 저장하면 나중에 buff가 추가돼도 날아가던 투사체의 피해가 중간에 바뀌지 않는다. buff가
투사체에 실시간으로 영향을 주는 콘텐츠가 필요해지면 별도 규칙으로 연다.

## 3. 상태 소유권

```text
SkillCatalog
└── ArcaneBolt의 불변 정의

PlayerActor
├── 보유한 SkillId
└── 장착한 공격 SkillId

RoomActor
├── 입장 때 복사한 전투 능력치와 장착 SkillId
├── Participant, Enemy
└── Projectile의 생성·이동·충돌·제거
```

- Projectile마다 Actor를 만들지 않는다.
- Projectile마다 Runtime timer를 만들지 않는다.
- 기존 `RoomSimulationTick` one-shot 사슬 하나가 모든 Projectile을 진행한다.
- PlayerActor는 발사 뒤 Projectile을 모르며, Room도 발사 중 PlayerActor를 조회하지 않는다.
- Projectile은 Room activation을 넘지 않고 DB에 저장하지 않는다.

Projectile을 별도 Actor로 만들면 작은 객체마다 mailbox, scheduling과 lifecycle 비용이 생기고 한 Room 안의
충돌 순서를 여러 Actor에 걸쳐 조정해야 한다. 현재 콘텐츠는 Room 하나의 순차 실행이 더 단순한 정답이다.

## 4. 도메인 모델 초안

### 4.1 공격 전달 방식

```cpp
struct AreaAttackBehavior
{
    std::uint64_t attack_percent{0};
    std::uint32_t range{0};
};

struct HomingProjectileAttackBehavior
{
    std::uint64_t attack_percent{0};
    std::uint32_t acquisition_range{0};
    std::uint32_t speed_per_tick{0};
    std::uint32_t hit_range{0};
    std::chrono::milliseconds lifetime{0};
};

using AttackBehavior = std::variant<AreaAttackBehavior, HomingProjectileAttackBehavior>;

struct SkillDefinition
{
    SkillId skill_id{};
    std::chrono::milliseconds cooldown{0};
    AttackBehavior behavior{};
};
```

`Slash`와 `ArcaneBolt`를 억지로 같은 필드에 맞추거나 virtual class 계층을 만들지 않는다. 두 값 타입을
`std::variant`로 묶고 Room의 cast 처리에서 `std::visit`으로 분기한다. 세 번째 공격 방식이 생기기
전에는 범용 effect graph, scripting과 ECS를 만들지 않는다.

### 4.2 런타임 Projectile

```cpp
struct ProjectileId
{
    std::uint32_t value{0};

    [[nodiscard]] bool operator==(const ProjectileId&) const noexcept = default;
};

struct Projectile
{
    ProjectileId id{};
    PlayerId owner{};
    SkillId skill_id{};
    EnemyId target{};
    ArenaPosition position{};
    std::uint32_t speed_per_tick{0};
    std::uint32_t hit_range{0};
    std::uint64_t damage{0};
    std::chrono::steady_clock::time_point expires_at{};
};
```

Room에 다음 상태를 둔다.

```cpp
std::uint32_t _next_projectile_id{1};
std::vector<Projectile> _projectiles;
```

- Projectile은 증가하는 ID 순서로 vector 뒤에 추가한다.
- `erase_if`는 남은 원소의 상대 순서를 유지하므로 ID 순서 처리를 유지한다.
- 한 Room activation 안에서 ID를 재사용하지 않는다.
- 다음 ID가 `uint32_t` 범위를 넘으려 하면 새 cast를 거절하고 wrap하지 않는다.

### 4.3 RoomConfig

```cpp
std::size_t max_active_projectiles{128};
```

Skill별 속도와 수명은 SkillCatalog가 소유한다. 전체 Room의 메모리 상한은 RoomConfig가 소유한다.

## 5. cast 처리 계약

`UseSkill`의 공통 검증 순서는 기존 규칙을 유지하되, 장착 기능이 붙은 뒤에는 장착 검사도 포함한다.

```text
Running phase 확인
→ battle deadline 확인
→ Participant 확인
→ request sequence 중복 확인
→ 생존 확인
→ SkillCatalog 조회
→ 장착 SkillId 확인
→ cooldown 확인
→ 공격 전달 방식 처리
```

### 5.1 Slash

현재와 같이 command turn 안에서 범위 내 모든 적에게 즉시 피해를 적용한다.

### 5.2 ArcaneBolt

```text
target 선택
→ active projectile 용량과 ID 확인
→ request sequence 확정
→ cooldown 시작
→ cast 시점 damage 계산
→ 시전자 현재 위치에 Projectile 생성
→ ProjectileSpawned event 추가
```

용량이 이미 찼거나 ID를 발급할 수 없으면 `ProjectileCapacityExceeded`를 반환한다.

- 용량 거절은 request sequence를 확정하지 않는다.
- 용량 거절은 cooldown을 소비하지 않는다.
- client는 새 request sequence로 나중에 다시 시도할 수 있다.
- target이 없는 `SkillWhiffed`는 반대로 sequence와 cooldown을 소비한다.

실패가 상태를 일부만 바꾸지 않도록 모든 검증을 끝낸 뒤 sequence, cooldown과 Projectile을 함께 갱신한다.

## 6. tick 처리 계약

현재 Room tick은 죽은 적 제거, 참가자 이동, 적 행동, wave/boss spawn 순서다. Projectile을 다음 위치에
삽입한다.

```text
1. 이전 turn에서 죽은 Enemy 제거
2. Participant 이동
3. Projectile을 ID 순서로 이동·충돌
4. boss가 죽었으면 즉시 Cleared로 종결
5. 살아 있는 Enemy 이동·공격
6. 예정된 wave와 boss spawn
7. digest 방출과 다음 tick 요청
```

Projectile을 Enemy 행동보다 먼저 처리한다. 해당 tick에 Projectile이 boss를 죽이면 boss와 다른 적은
그 뒤에 공격하지 않는다.

### 6.1 Projectile 하나의 처리 순서

```text
expires_at <= observed_at ? ProjectileRemoved(Expired)
→ 살아 있는 target 조회 실패 ? ProjectileRemoved(TargetLost)
→ 이동 전 충돌 범위 확인
→ 필요하면 target 현재 위치로 moveToward
→ 이동했으면 ProjectileMoved
→ 이동 후 충돌 범위 확인
→ 충돌이면 피해와 제거 event 생성
```

만료를 target 유실보다 먼저 확인한다. 같은 tick에 둘 다 만족하면 removal reason은 `Expired`다.

### 6.2 여러 Projectile의 경합

- ProjectileId가 작은 것부터 처리한다.
- 앞 Projectile이 target을 죽이면 뒤 Projectile은 같은 tick에 `TargetLost`로 제거된다.
- 앞 Projectile이 boss를 죽이면 현재 충돌의 인과 event를 모두 추가한 뒤 나머지 Projectile 진행을 멈춘다.
- terminal 결과를 만들 때 남은 Projectile 상태는 폐기한다. client는 `BattleCleared` 또는 `BattleFailed`를
  받으면 화면의 모든 Projectile을 함께 비운다.

## 7. 관찰 event와 순서

```cpp
struct ProjectileSpawned
{
    ProjectileId projectile{};
    PlayerId owner{};
    SkillId skill_id{};
    EnemyId target{};
    ArenaPosition position{};
};

struct ProjectileMoved
{
    ProjectileId projectile{};
    ArenaPosition position{};
};

enum class ProjectileRemovalReason : std::uint8_t
{
    Hit,
    TargetLost,
    Expired,
};

struct ProjectileRemoved
{
    ProjectileId projectile{};
    ProjectileRemovalReason reason{ProjectileRemovalReason::Expired};
};
```

`BattleEventKind`의 기존 값은 바꾸지 않고 끝에 새 값을 추가한다.

```text
11 ProjectileSpawned
12 ProjectileMoved
13 ProjectileRemoved
```

충돌의 event 순서는 다음과 같다.

```text
선택적 ProjectileMoved
→ EnemyDamaged
→ 선택적 EnemyDied
→ ProjectileRemoved(Hit)
→ 선택적 BattleCleared
```

`EnemyDamaged`는 이미 owner와 SkillId를 포함하므로 별도의 `ProjectileHit` event를 중복해서 만들지 않는다.

## 8. 용량과 비용 계약

- `_projectiles`는 `max_active_projectiles`만큼 미리 reserve한다.
- cast는 상한을 넘겨 vector를 키우지 않는다.
- 한 tick은 Projectile 하나당 최대 이동 event 하나를 만든다.
- 충돌은 이동, 피해, 사망과 제거를 합쳐 최대 event 네 개를 만든다.
- Room 생성자의 pending event reserve 계산에 `max_active_projectiles`의 최악 event 수를 포함한다.
- digest 최대 payload 검사는 기존 `ProtocolRoomResultSink` 경계를 그대로 사용한다.

초기 상한 128에서는 Projectile 이동만으로 tick마다 event 128개가 생길 수 있다. 구현 후 다음을 측정한다.

- tick당 active Projectile p50/p99/max
- Projectile spawned, hit, target-lost와 expired 수
- Projectile capacity rejection 수
- tick 실행 시간과 budget 초과
- Projectile event bytes와 digest fanout bytes

상한과 event 양이 실제 병목이 되기 전에는 별도 Projectile worker, 공간 partition과 delta compression을
추가하지 않는다.

## 9. 종료와 lifecycle

- `Cleared`, `Failed`, 참가자가 모두 나간 Room과 passivation에서 Projectile은 더 진행하지 않는다.
- terminal `RoomResult`는 다음 tick을 요청하지 않는다.
- terminal 전이에서 `_projectiles`를 비운다.
- stale `RoomSimulationTick`이 terminal 뒤 도착해도 `WrongPhase`로 상태를 바꾸지 않는다.
- Room이 passivate되면 vector와 모든 Projectile은 Room 상태와 함께 파괴된다.
- disconnect한 시전자의 이미 발사된 Projectile은 계속 진행한다. 피해 소유자는 cast 시점 `PlayerId`로
  남으며, clear 보상은 충돌 기여가 아니라 terminal 시점 참가자 목록으로 결정한다.

마지막 규칙은 Projectile을 Player session 수명에 묶지 않는다. disconnect 때 기존 투사체를 모두 찾고
취소하는 비용과 추가 event를 만들지 않는다.

## 10. 구현 순서

각 단계는 build와 관련 단위 테스트가 통과하는 상태로 끝낸다. 한 단계가 끝나면 diff와 테스트를 확인한
뒤 다음 단계로 넘어간다.

### Step 0 — 계약 고정

- 이 문서를 읽고 게임 규칙과 비범위를 확정한다.
- `ArcaneBolt` 이름과 초기 수치를 확정한다.
- target 자동 선택과 유도형이라는 정책을 확정한다.

완료 조건:

- cast부터 terminal까지 상태와 event 순서를 문서만으로 설명할 수 있다.
- 구현 중 새 정책 질문이 나오면 먼저 이 문서를 수정한다.

### Step 1 — 공격 정의를 variant로 분리

대상:

- `src/game/skill_catalog.cppm` (named module `snf.game.skill_catalog`)
- `tests/skill_catalog_test.cpp`

작업:

- `findSkill`과 `skillDamage`, behavior/정의 타입만 모듈 인터페이스로 export
- 카탈로그의 정의 생성 helper와 조회 분기는 모듈 내부로 숨김
- `AreaAttackBehavior`, `HomingProjectileAttackBehavior` 추가
- `AttackBehavior` 추가
- 기존 Slash를 area 정의로 이전
- ArcaneBolt 정의 추가
- attack percent에서 피해를 계산하는 공통 함수를 유지하거나 새 값 타입에 맞게 정리

완료 조건:

- Slash와 ArcaneBolt를 각각 조회할 수 있다.
- 존재하지 않는 SkillId는 `nullopt`다.
- 두 behavior type과 피해량, cooldown, range를 테스트한다.
- Room의 기존 Slash 테스트가 전부 그대로 통과한다.

이 단계에서는 Projectile 상태와 event를 만들지 않는다.

### Step 2 — Projectile 값 타입과 target 선택

대상:

- 새 `include/snf/game/projectile.hpp`
- `include/snf/game/room.hpp`
- `src/game/room.cpp`
- `tests/room_test.cpp`

작업:

- `ProjectileId`, `Projectile` 추가
- Room에 Projectile vector, next ID와 capacity 추가
- 가장 가까운 생존 적을 찾는 결정적 helper 추가
- ArcaneBolt cast가 Projectile을 생성하도록 구현
- 아직 tick 이동은 구현하지 않는다.

완료 조건:

- 발사 위치가 시전자 위치다.
- 가장 가까운 적과 동거리 tie-break가 정확하다.
- 발사 직후 Enemy HP는 바뀌지 않는다.
- target이 없으면 whiff되고 Projectile은 없다.
- 중복 request sequence는 Projectile을 추가하지 않는다.

### Step 3 — 이동, 충돌과 제거

대상:

- `include/snf/game/room.hpp`
- `src/game/room.cpp`
- `tests/room_test.cpp`

작업:

- `advanceProjectiles()` 추가
- tick 순서에 Projectile 단계 추가
- hit, target lost와 expiry 처리
- boss kill terminal 처리
- capacity와 ID exhaustion 처리

완료 조건:

- 여러 tick에 걸쳐 target을 추적한다.
- 충돌 전에는 피해가 없고 충돌 때 한 번만 피해를 준다.
- target 사망과 lifetime 만료 시 정확한 이유로 제거된다.
- 같은 target을 향하는 여러 Projectile의 결과가 ID 순서로 결정적이다.
- Projectile boss kill 뒤 Enemy가 공격하지 않는다.
- terminal과 stale tick 뒤 Projectile이 남지 않는다.

### Step 4 — BattleDigest와 protocol

대상:

- `include/snf/game/room_result.hpp`
- `src/server/protocol_room_result_sink.cpp`
- `tests/protocol_room_result_sink_test.cpp`
- `tests/tcp_server_integration_test.cpp`

작업:

- 세 Projectile event를 `BattleEvent` 끝에 추가
- event size 계산과 big-endian encoding 추가
- digest payload 상한과 event 순서 테스트 추가
- 실제 TCP에서 spawn, move, damage와 remove를 관찰

완료 조건:

- 기존 event tag 값이 바뀌지 않는다.
- 새 event의 wire byte layout이 테스트로 고정된다.
- 실제 TCP에서 cast와 hit 사이에 최소 한 번의 이동을 관찰한다.
- digest sequence가 연속이다.

### Step 5 — Python client 시각화

대상:

- `game-client/snf_wire.py`
- `game-client/snf_world.py`
- `game-client/snf_play.py`
- `game-client/test_client.py`

작업:

- 새 event tag와 payload parser 추가
- World에 Projectile entity map 추가
- spawn, move, remove 반영
- 위치 보간과 간단한 원형 또는 발광 projectile 렌더링
- terminal 전이 때 Projectile map 정리

완료 조건:

- 화면에서 Player로부터 target까지 Projectile이 이동한다.
- hit, target lost와 expiry 모두 화면에 잔상이 남지 않는다.
- headless parser와 World 단위 테스트가 통과한다.

### Step 6 — Player의 보유·장착 상태

Projectile 전투가 독립적으로 완성된 뒤 Player 진행 상태에 연결한다.

```cpp
struct SkillLoadout
{
    std::vector<SkillId> owned_skill_ids{};
    SkillId equipped_skill_id{};
};
```

- 새 Player는 Slash를 보유하고 장착한다.
- 보유 목록은 SkillId 오름차순이며 중복이 없다.
- 보유하지 않은 Skill은 장착할 수 없다.
- Room 입장 시 `CombatStats`와 장착 SkillId를 함께 복사한다.
- Room은 client의 SkillId가 입장 스냅샷의 장착 SkillId와 같은지 확인한다.
- 입장 뒤 장착 변경은 현재 Room에 반영하지 않는다.

완료 조건:

- 서로 다른 Player가 Slash와 ArcaneBolt를 각각 장착해 같은 Room에서 사용한다.
- 미장착 SkillId를 보낸 client는 피해나 Projectile을 만들지 못한다.

### Step 7 — 재화 구매

ProductCatalog의 추상적인 `grant_count`를 구체적인 지급 값으로 확장한다.

```cpp
struct AddPurchasedItemCountReward
{
    std::uint64_t item_count{0};
};

struct AddOwnedSkillReward
{
    SkillId skill_id{};
};

using ProductReward = std::variant<AddPurchasedItemCountReward, AddOwnedSkillReward>;
```

- ArcaneBolt 상품은 `AddOwnedSkillReward{ARCANE_BOLT_SKILL_ID}`를 지급한다.
- 이미 보유하면 `AlreadyOwned`이고 재화를 차감하지 않는다.
- 재화 차감과 보유 목록 추가는 하나의 Player command 안에서 함께 성공하거나 함께 실패한다.
- 기존 Actor 수명 범위 idempotency와 영속 보유 목록의 중복 방어를 모두 유지한다.

완료 조건:

- 정상 구매, 재화 부족, 중복 구매, 같은 key replay와 다른 상품 key conflict를 테스트한다.

### Step 8 — 장착 protocol과 영속화

- `EquipSkillCommand`와 `EquipSkillResponse`를 추가한다.
- `PlayerStateComponent::Skills`를 추가한다.
- `PlayerRecord`에 `SkillLoadout`을 추가한다.
- MySQL `snf_player_skills(player_id, skill_id)`와 `equipped_skill_id`를 추가한다.
- Player 기본 row와 기존 row는 Slash 보유·장착으로 복원한다.
- main Player row와 skill rows는 하나의 DB transaction에서 저장한다.

완료 조건:

```text
인증
→ ArcaneBolt 구매
→ ArcaneBolt 장착
→ disconnect와 저장
→ reconnect와 load
→ Room 입장
→ ArcaneBolt 발사·이동·충돌 관찰
```

InMemory repository와 MySQL integration에서 같은 round trip을 검증한다.

## 11. 단계별 권장 커밋

```text
docs(game): define the projectile skill contract
refactor(game): model attack behavior variants
feat(game): spawn targeted room projectiles
feat(game): advance and resolve room projectiles
feat(protocol): publish projectile battle events
feat(client): render projectile battle events
feat(game): own and equip attack skills
feat(game): grant skills from purchases
feat(persistence): persist player skill loadouts
```

## 12. 전체 검증 목록

### 정상 경로

- ArcaneBolt가 가장 가까운 적을 선택한다.
- Projectile이 여러 tick 동안 이동한다.
- 충돌 때 정의된 피해를 한 번 적용한다.
- boss를 죽이면 clear와 보상을 한 번만 만든다.

### 결정성

- 동거리 target은 작은 EnemyId다.
- Projectile은 작은 ProjectileId부터 처리한다.
- 같은 입력과 tick 시각에서 같은 event 순서가 나온다.

### 잘못된 요청

- unknown SkillId
- 미장착 SkillId
- cooldown 중 요청
- 중복 request sequence
- 죽은 Participant의 cast
- Room 밖 또는 terminal phase의 cast

### lifecycle

- target이 먼저 죽음
- 시전자가 disconnect
- Projectile 비행 중 deadline 도달
- Projectile 비행 중 boss clear
- 모든 참가자 패배
- Room passivation 뒤 stale tick 도착

### 용량

- active Projectile 상한
- Projectile ID exhaustion
- pending event reserve
- digest payload 상한
- 느린 client가 있어도 건강한 참가자의 digest sequence 연속

## 13. 비범위

- client가 보내는 aim vector와 mouse 좌표
- 자유 각도 직선 이동과 fixed-point 좌표
- swept collision과 벽 충돌
- projectile끼리 충돌하거나 상쇄
- 관통, bounce, chain, splash와 상태 효과
- ProjectileActor와 별도 물리 Worker
- generic effect graph, ECS와 skill scripting
- 진행 중 Room reconnect와 Projectile 영속화

이 항목들은 ArcaneBolt vertical slice가 정상·포화·disconnect·terminal까지 끝난 뒤, 새로운 콘텐츠가 실제로
요구할 때 하나씩 연다.
