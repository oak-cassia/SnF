# SnF

SnF는 C++20 Actor 모델로 온라인 게임 콘텐츠의 상태 소유권, 명령 순서와 실패 경계를
검증하는 MORPG 서버 프로젝트다. MMORPG 규모를 흉내 내기보다 Player·Zone·Room의 작은
vertical slice를 끝까지 연결하고, 과부하·재접속·저장·종료 상황에서도 상태가 어떻게
종결되는지를 명시하는 데 집중한다.

## 계층

```text
snf_game                       Player / Zone / Room 상태 기계와 값
  ↑ 아무것도 링크하지 않는다
snf_server_runtime             Binding, ingress, routing, persistence, protocol
  ↑
snf_mysql_player_repository    MySQL을 아는 유일한 target
```

의존은 항상 바깥에서 안쪽으로 간다. `snf_game_tests`가 `snf_game`만 링크하고,
`snf_game_layer`가 게임 트리의 include를 검사해 이를 강제한다.

## 핵심 구조

```text
Client
  ↓ binary frame
epoll Network Runtime
  ↓ typed command
ProtocolGateway / CommandRouter
  ↓
Sharded Actor Runtime
  ├── PlayerActor  ── PlayerPersistenceService ── Repository
  ├── ZoneActor
  └── Room
  ↓ typed result
Bounded OutboundChannel
  ↓
Network Runtime
```

- `ActorKey`를 고정 Worker에 shard하고 Actor별 FIFO mailbox를 처리한다. 같은 Actor의 mutable
  상태는 동시에 실행되지 않는다.
- 외부 I/O를 기다리는 coroutine은 해당 Actor만 suspend한다. completion producer는 coroutine
  handle이나 Actor 객체를 보유하지 않는다.
- ingress, mailbox, in-flight operation, persistence와 outbound queue에는 모두 상한과 포화
  정책이 있다.
- protocol frame은 Actor까지 전달하지 않고 typed command/result 경계에서 변환한다.
- shutdown은 새 입력 차단, Actor와 continuation drain, Player snapshot flush, outbound drain
  순서로 진행한다.

상세한 현재 구조와 트레이드오프는 [서버 아키텍처](docs/server-architecture-draft.md), Actor Runtime의
구성요소와 실행 흐름은 [Actor Runtime 아키텍처](docs/actor-runtime-architecture.md), Zone·Room·Player의
협력 방식은 [Actor 상호작용 아키텍처](docs/actor-interaction-architecture.md), coroutine 수명과 경합
규칙은 [Coroutine Actor 계약](docs/coroutine-actor-contract.md), 전체 종료 순서는
[Runtime Lifecycle 계약](docs/runtime-lifecycle-contract.md), Actor의 tick·timeout 예약 정책은
[Actor 주도 Timer Scheduling](docs/actor-driven-timer-scheduling.md), Actor 간 메시지와 게임 시간
기준은 [Actor 간 메시지와 게임 시간 결정](docs/actor-messaging-and-game-time.md)을 기준으로 한다.

## 구현된 vertical slice

### Player session과 economy

- 인증 전 provisional Actor에서 영속 Player Actor로 route 전환
- `PlayerActor`가 session과 economy 상태를 단독 소유
- 상품 가격, 잔액, 지급과 Actor 수명 범위 idempotency를 한 turn에서 판정
- dirty snapshot을 bounded queue로 제출하고 Player별 save를 coalesce·직렬화
- disconnect/save/reconnect 뒤 economy와 마지막 Zone 위치 복원
- 기본 in-memory adapter와 bounded Worker Pool을 사용하는 MySQL 8 adapter

상세 소유권 규칙과 durability 한계는
[Player 상태 소유권과 persistence 계약](docs/player-state-ownership-contract.md)에 있다.

### Zone

- enter/move/leave, route epoch, periodic tick과 AOI
- stale route 폐기, 빈 Zone passivation
- source leave → target enter → route publish 순서의 cross-zone handoff
- target 실패 시 source 복구, disconnect·shutdown 중 cleanup

상세 실패 계약은 [Cross-Zone Handoff 계약](docs/cross-zone-handoff-contract.md)에 있다.

### Battle Room과 street 성장

- `Waiting → Running → Cleared | Failed` 상태 기계, 100ms one-shot tick 사슬과 별도 전투 deadline timer
- Room은 비영속 100×100 정수 Arena의 참가자·적 좌표와 HP를 소유한다. client는 이동 의도 또는
  skill id와 request sequence만 보내며 damage, targeting, cooldown과 생사는 서버가 판정한다
- 같은 request sequence는 damage를 두 번 적용하지 않고, cooldown은 turn 시작 시각과 비교하는
  deadline이다 (`ActorContext::observedAt`)
- `SetMoveIntent`는 persistent 8방향 의도만 바꾸고 다음 tick이 좌표를 이동한다. `Slash`는 사거리
  안의 모든 생존 적을 EnemyId 순서로 공격하고, 적은 가장 가까운 생존 참가자를 추격·공격하며
  동률은 작은 PlayerId다
- 기본 전투 밸런스는 minion HP 60·이동속도 1, boss HP 500이다
- `UseSkill`은 현재 좌표에서 즉시 판정하고 요청자에게 `SkillAcknowledged`를 보낸다. 좌표·damage·
  death·spawn은 인과 순서를 보존한 `BattleDigest(request_id = 0)`로 전 참가자에게 fanout한다
- clear 시 참가자마다 `tryTell`로 street 경험치 전달, 미상주 Player는 레코드를 먼저 로드
- 누적 경험치만 저장하고 레벨과 공격/체력은 파생 (레벨당 +10% 선형, 상한 30)
- `RoomJoin`/`BattleStart`/`SetMoveIntent`/`UseSkill` 요청과, 요청 없이 나가는 `BattleDigest`·
  `BattleCleared`·`BattleFailed` 알림. `BattleFailed`는 `Deadline`과 `ParticipantsDefeated`를 구분한다
- Arena 좌표는 Room 종료와 함께 사라지며 `ReturnedToZone`은 입장 전에 저장한 Zone 좌표를 복원한다

충돌, 장애물, pathfinding, projectile, Room AOI와 진행 중 resync snapshot은 없다. matchmaking도
없으며 클라이언트가 room id를 지정한다. 기존 `SkillApplied` wire 번호는 예약 상태로 남아 있다.

## Actor 모델로 검증하는 것

| 문제 | 설계와 검증 |
| --- | --- |
| 동일 Entity의 동시 변경 | Actor별 single writer와 FIFO mailbox |
| 느린 외부 작업 | 해당 Actor coroutine만 suspend하고 같은 Worker의 다른 Actor는 진행 |
| queue 포화 | 시작 전 reservation 또는 typed rejection으로 메모리 상한 유지 |
| 늦은 completion | `{ActorKey, Incarnation, TaskId}` 불일치 결과 폐기 |
| Actor 간 전환 | route epoch, bounded completion과 명시적 보상 |
| 종료 경합 | ingress close 후 mailbox·continuation·persistence·outbound 순서대로 drain |

Actor는 무조건 처리량을 높이는 도구가 아니다. 한 hot Actor는 단일 Worker 처리량에 제한되고,
Actor 사이 원자적 변경에는 별도 상태 기계와 보상이 필요하다. 이 프로젝트는 그 비용까지
테스트와 metric으로 드러내는 것을 목표로 한다.

## 검증 기준선

로컬 Docker Release 기준 200 connections, 8 Zones, 12초, 연결당 20 req/s에서
48,000/48,000 gameplay 응답, timeout·queue overflow·tick overrun 0, gameplay p99
`3.705 ms`를 기록했다. 단일 Zone에 200명을 집중시킨 실험에서는 한 Worker로 처리량이
몰리는 hot Actor 한계도 확인했다.

테스트는 Actor ordering, coroutine completion/cancel 경합, outbound 포화, Player persistence,
Zone/Room 상태 기계, 실제 TCP 왕복과 graceful shutdown을 포함한다. Debug 외에
ASan·UBSan과 TSan preset을 제공한다.

## 빌드와 실행

서버는 Linux `epoll`을 사용한다. macOS에서는 Docker container에서 빌드한다.

```bash
docker build -t snf-server-dev .

docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev

cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

서버와 Zone 부하 시나리오:

```bash
./build/debug/snf_server

./build/release/snf_load_client \
  --scenario zone \
  --connections 200 \
  --players-per-zone 25 \
  --duration 12 \
  --requests-per-second 20
```

Room 전투 부하는 방별 최대 4명으로 나누고, 모든 참가자가 입장한 뒤 방별 leader 하나가 전투를
시작한다. worker 확장 비교는 서버의 `SNF_ACTOR_WORKER_COUNT`를 1·2·4로 바꿔 같은 명령을 반복한다.

```bash
SNF_ACTOR_WORKER_COUNT=1 ./build/release/snf_server

./build/release/snf_load_client \
  --connections 128 \
  --scenario battle \
  --players-per-room 4 \
  --duration 30 \
  --requests-per-second 20
```

MySQL adapter는 다음 환경 변수가 있을 때 선택된다. MySQL 통합 테스트는 별도의 test database를
사용해야 하며 `SNF_MYSQL_TEST_HOST`가 없으면 skip된다.

```bash
SNF_MYSQL_HOST=127.0.0.1 \
SNF_MYSQL_USER=snf \
SNF_MYSQL_PASSWORD=secret \
SNF_MYSQL_DATABASE=snf \
./build/debug/snf_server
```

Sanitizer 검증:

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan --output-on-failure

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

## 개발 범위

현재 Runtime, infra와 Battle Room vertical slice의 범위는 고정한다. 구현 범위와 완료 조건은
[개발 로드맵](docs/development-roadmap.md)에 기록한다.

콘텐츠의 상태 모델, 규칙, 메시지 흐름과 핵심 C++ 구현은 직접 수행한다. 반복적인 테스트 보강,
리팩터링과 문서 정리에는 LLM을 사용하며, 변경은 코드 리뷰와 자동화 테스트 및 필요한 부하
측정을 통과한 뒤 반영한다.
