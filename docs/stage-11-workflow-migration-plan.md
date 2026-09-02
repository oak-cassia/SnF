# 11단계 — Application workflow 이전, production 전환과 legacy 제거

> 문서 상태: **확정 실행 계획**. 승인 2026-09-03.
>
> 상위 단계와 완료 조건은 [개발 로드맵 11단계](./development-roadmap.md), 런타임 불변식은
> [Unified Worker Runtime](./architecture/unified-worker-runtime.md)를 따른다. 이 문서는 11A~11K의
> 실행 기준이다.
>
> 10단계 결과는 [품질 게이트 리포트](./worker-runtime-quality-gates.md)에 있고, §11의 남은 한계 3건이
> 이 단계의 11I에서 닫힌다.

## 사전 확인 사항

계획 수립 시점에 직접 확인한 사실이다.

| 사실 | 근거 | 영향 |
| --- | --- | --- |
| 신규 경로 inbound는 `Ping` 하나만 처리했다 | `src/adapter/game_request_sink.cpp` (11A 이전) | 최대 작업량. frame 8종 라우팅 신설 |
| **outbound는 이미 완성됐다** | `protocol_encoder.hpp:30`~50, `to_effects.hpp:64`~68 | 응답 매핑 재작성 불필요 |
| Player/Zone/Room adapter와 tick·deadline payload가 이미 있다 | `include/snf/adapter/{player,zone,room}_actor_adapter.hpp`, `game_payloads.hpp:28`~80 | domain turn 실행부 재사용 |
| transition state machine은 신규 경로에 없다 | Entering/InRoom/Returning/correlation grep 0건 | 로드맵 1~5번 체크박스의 구현 대상 |
| `PlayerSessionDirectory`는 mutex 공유 map이다 | `player_session_directory.hpp:65` | 그대로 못 옮긴다. 아키텍처 §4 위반 |
| `RequestSink::onConnectionClosed`가 owner-thread hook으로 있다 | `request_sink.hpp` | 세션 정리 자리 확보됨 |
| **`Rejected`/`Invalid`는 연결을 끊는다** | `worker.cpp:1109`~1118 → `forceClose` | 도메인 실패를 여기에 매핑하면 계약이 깨진다 |
| `beginGracefulClose`는 항상 `forceClose`로 끝난다 | `worker.cpp:2743`~2777 | actor가 요청한 close도 세션을 반드시 해제한다 |
| `closeConnection`은 remote owner로 라우팅된다 | `worker.cpp:525`~541 | 다른 Worker의 연결도 effect로 닫을 수 있다 |
| DB 연산 격차 없음 | legacy `asyncLoad`/`asyncSave` ↔ `LoadPlayerRequest`/`SavePlayerRequest` | DB 확장 불필요 |
| domain 타입은 `snf_game`에 있고 이름만 `snf::server`다 | `include/snf/game/player.hpp:12` | 제거 대상은 `snf_server_runtime`뿐 |
| 제거 대상 legacy | `.cpp` 약 7,100 LOC + 헤더 + 테스트 17개 파일 | 전환과 제거를 같은 커밋에 두지 않는다 |
| `Distribution` 사용처는 전부 legacy | runtime/server 헤더 + `server_main.cpp` + mysql repo + `distribution_test` | 10단계에서 미룬 정리가 여기서 풀린다 |
| parity oracle이 이미 존재한다 | `tests/tcp_server_integration_test.cpp` | parity를 손으로 정의하지 않아도 된다 |
| `LoadClient`는 무한 누적한다 | `load_client.hpp:53`~55, `vector<duration>` 3개 | 게이트에 쓰려면 bounded로 고쳐야 한다 |
| `LoadScenario`는 Ping/Zone/Battle | `load_scenario.hpp:9`~11 | 프로토콜이 서빙되면 10단계가 미룬 이유가 사라진다 |
| `snf_server`는 `GameServer`를 구동한다 | `server_main.cpp:192` | production 전환 대상 |

## 핵심 설계 결정

### D1. Transition owner = PlayerActor, Coordinator Actor 미채택

계약 문서가 target owner 결정을 이 단계로 위임했고([room-entry-handoff-contract.md:3](./room-entry-handoff-contract.md)),
아키텍처 §12가 절차를 규정한다. §12 규칙 2를 적용한다.

```text
PlayerActor
  workflow state + correlation ID
Zone/Room
  ↕ mailbox message
ActorSlot.blocked 사용 안 함
```

근거: 계약의 상태(`Stable`/`Transferring`/`Entering`/`InRoom`/`Returning`)는 전부 한 Player에 귀속되고,
connection마다 진행 중 transition은 최대 하나다(room 계약 §2). 제3자가 조회·취소하지 않고 transition이
PlayerActor보다 오래 살지 않으므로 §12 규칙 3(Coordinator)의 조건을 하나도 만족하지 않는다.

correlation ID는 `ActorSlot.blocked`를 쓰지 않는다. actor-to-actor 응답은 suspend하지 않으므로(§12 첫 문장)
INV-06과 무관하게 workflow state 안의 별도 필드다.

party/group transition처럼 여러 Player를 독립적으로 조정하는 요구가 생기면 그때 Coordinator를 검토한다.

### D2. Session identity — 공유 directory를 없애고 양방향으로 쪼갠다

| 방향 | 소유자 | 이유 |
| --- | --- | --- |
| connection → player | owner worker의 sink-local map (owner thread 전용) | worker가 이미 connection owner다 |
| player → connection | PlayerActor가 보유한 `ConnectionRef` | 서버 주도 unsolicited 프레임의 대상이 여기다 |
| PlayerConflict | PlayerActor의 turn 결정 | 충돌 상대 connection이 다른 Worker일 수 있다 |

`WorkerGroup`은 factory를 worker index마다 1회 호출해 sink를 별개로 소유하므로(`worker_group.cpp:78`,
`worker_group.hpp:78`) owner thread 전용 map이 성립한다. 다만 단일 `Worker` 생성자는 `RequestSink&`를
받으므로 타입 시스템이 공유를 막지 않는다. 계약을 `request_sink.hpp`에 명시하고 sink가 owner thread를
debug assertion으로 고정한다.

### D3. 인증 전후로 ActorKey가 달라진다

인증 후 `entity = player_id`다. 인증 전 game frame은 PlayerActor를 만들지 않고 sink가 거절한다.

### D4. 도메인 실패는 `Rejected`로 반환하지 않는다

```text
mailbox full / 용량 초과   → Rejected  (overload는 연결 종료가 맞다)
프로토콜/프레임 순서 위반  → Invalid
도메인 실패(좌석 없음 등)  → Accepted + 실패 프레임
```

### D5. Parity oracle = 기존 통합 테스트를 신규 경로로 포팅

`tcp_server_integration_test.cpp`의 시나리오를 worker 경로용으로 복제하고, legacy 테스트는 11J까지 그대로
돌린다. 두 경로가 같은 시나리오를 통과하는 것이 parity의 정의다.

### D6. 순서: 서빙 → workflow → 전환 → 게이트 → 제거

11H에서 런타임 authority를 한 번에 바꾸되 legacy 코드는 11I 통과 전까지 rollback 가능한 상태로 남긴다.
포트를 분리해 legacy와 Worker를 동시에 production authority로 두지 않는다. 상태·DB·세션 authority가 두
군데가 되면 전환 검증이 불가능해진다.

## 서브 스텝

### Phase 1 — 서빙 격차 (production 미변경)

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 11A | `feat(adapter): bind sessions and authenticate on the worker path` | 신규 `tests/worker_session_test.cpp` |
| 11B | `feat(adapter): route player commands to the owning PlayerActor` | Purchase / EquipSkill |
| 11C | `feat(adapter): route zone commands and AOI results` | EnterZone / Move / LeaveZone |
| 11D | `feat(adapter): route room and battle commands` | RoomJoin / BattleStart / UseSkill / SetMoveIntent / RoomLeave |

### Phase 2 — Workflow

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 11E | `feat(adapter): own room entry and return in the PlayerActor workflow state` | room 계약 §3·§4 |
| 11F | `feat(adapter): own cross-zone transition in the same workflow state` | cross-zone 계약 §3·§4 |
| 11G | `test(adapter): match failure, disconnect and shutdown terminals to the contracts` | room §5·§6 + cross-zone §5·§6, 계약 조항별 대조표 |

### Phase 3 — Production 전환과 게이트

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 11H | `feat(server): serve production traffic from the Worker runtime` | `server_main.cpp` → `WorkerGroup` |
| 11I | `test(worker): re-run the stage 10 gates on the production path` | production 기본값 + MySQL 실측 + `LoadClient` bounded |

### Phase 4 — 제거와 문서

| 스텝 | 커밋 | 비고 |
| --- | --- | --- |
| 11J | `refactor: remove the legacy ActorRuntime, bindings and shared outbound` | `snf_runtime` + `snf_server_runtime` legacy + `Distribution` + 테스트 17개. `snf_game`은 유지 |
| 11K | `docs: retarget the README and close stage 11` | README 배너 제거, 실제 코드 링크와 새 측정값 |

의존: 11A→11B·11C·11D → 11E→11F→11G → 11H→11I→11J→11K.

## 진행 상황

| 스텝 | 상태 | 비고 |
| --- | --- | --- |
| 11A | **완료** | 아래 "11A 결과" 참고 |
| 11B~11K | 미착수 | |

### 11A 결과

- sink가 session identity의 connection → player를 소유하고, PlayerActor가 `ConnectionRef`를 보유한다.
- legacy `PlayerAttachResult` 6개 결과값에 각각 owner를 배정하고 `worker_session_test.cpp`로 고정했다.
- session entry는 connection id로 키를 잡고 generation을 함께 검증한다. close 통지를 놓친 stale entry가
  재사용된 slot에 이전 player의 세션을 넘기지 않고 fail closed된다.
- **의도적 동작 변경 2건**
  - 인증 전 `Ping`을 sink가 직접 답한다. 인증되지 않은 연결이 actor slot을 할당할 수 없게 하려는 것이다.
  - 그 결과 legacy의 provisional actor 부작용(ping한 연결은 이후 인증이 막힘)은 **보존하지 않는다.**
- `RequestSink`에 "Worker마다 1 인스턴스" 계약을 명시하고 sink가 owner thread를 debug assertion으로 고정했다.
- 관측용 live session count만 atomic mirror다. map 자체는 다른 thread에서 읽지 않는다. 이 규칙을 어긴
  **테스트 자신이 TSan data race로 잡혔고**, 계약대로 고쳤다.

## 검증

각 스텝마다 Docker 안에서:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc 'cmake --build --preset debug && ctest --preset debug --output-on-failure'
```

Phase 2·3 직후에는 세 preset 전부:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc 'cmake --build --preset tsan && TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan -L worker --output-on-failure'
```

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc 'cmake --build --preset asan-ubsan && ctest --preset asan-ubsan -L worker --output-on-failure'
```

## 리스크

- **cross-worker transition ordering.** Player와 Zone/Room이 다른 worker면 transition 응답이 remote inbox를
  거친다. remote tell은 10단계에서 검증됐지만 계약의 순서·backpressure 의미를 재확인해야 한다.
- **completion slot 예약 개념이 신규 경로에 없다.** room 계약 §3의 예약은 legacy outbound reservation
  기반이다. 신규 경로의 대응물이 무엇인지(또는 불필요한지)를 11E에서 먼저 결론낸다.
- **D4 함정.** 도메인 실패를 잘못 매핑하면 부하 중 연결이 끊기고 parity 실패로 나타난다.
- **11I의 MySQL 실측이 처음이다.** `mysql_close()` boundedness를 포함해 새로 드러날 문제가 있을 수 있다.
- **11J 후 `snf::server::Player` 이름과 `snf_game` 위치의 불일치가 남는다.** 네임스페이스 정리는 범위 밖이다.

## 범위 밖

- `snf::server::Player` → `snf::game::Player` 네임스페이스 정리
- durable workflow system (§12 규칙 4)
- actor live migration, dynamic worker scaling, idle passivation
- matchmaking, group 동시 입장, 프로세스 간 이동, mid-battle reconnect (계약 §1의 명시적 범위 밖)
