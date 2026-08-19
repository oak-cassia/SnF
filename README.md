# SnF

SnF는 C++20 Actor 모델로 온라인 게임 콘텐츠의 상태 소유권, 명령 순서와 실패 경계를
검증하는 MORPG 서버 프로젝트다. MMORPG 규모를 흉내 내기보다 Player·Zone·Party의 작은
vertical slice를 끝까지 연결하고, 과부하·재접속·저장·종료 상황에서도 상태가 어떻게
종결되는지를 명시하는 데 집중한다.

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
  └── PartyActor
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

상세한 현재 구조와 트레이드오프는 [서버 아키텍처](docs/server-architecture-draft.md), coroutine
수명과 경합 규칙은 [Coroutine Actor 계약](docs/coroutine-actor-contract.md), 전체 종료 순서는
[Runtime Lifecycle 계약](docs/runtime-lifecycle-contract.md), Actor의 tick·timeout 예약 정책은
[Actor 주도 Timer Scheduling](docs/actor-driven-timer-scheduling.md), Actor 간 메시지와 게임
시간 기준은 [Actor 간 메시지와 게임 시간 결정](docs/actor-messaging-and-game-time.md)을 기준으로 한다.

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

### Party

- Party별 FIFO membership 변경과 정렬된 member snapshot
- capacity 초과를 typed `PartyFull`로 응답
- membership epoch으로 stale leave 차단
- 마지막 member가 나간 뒤 mailbox-safe passivation

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
Party/Zone 상태 기계, 실제 TCP 왕복과 graceful shutdown을 포함한다. Debug 외에
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

## 개발 범위와 다음 단계

현재 Runtime과 infra 범위는 고정한다. 다음 단계는 새로운 범용 서버 기능이 아니라 Party와
Player 상태를 실제로 소비하는 작은 인스턴스 콘텐츠다. 구체적인 범위와 완료 조건은
[개발 로드맵](docs/development-roadmap.md)에만 기록한다.

콘텐츠의 상태 모델, 규칙, 메시지 흐름과 핵심 C++ 구현은 직접 수행한다. 반복적인 테스트 보강,
리팩터링과 문서 정리에는 LLM을 사용하며, 변경은 코드 리뷰와 자동화 테스트 및 필요한 부하
측정을 통과한 뒤 반영한다.
