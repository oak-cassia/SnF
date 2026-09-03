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

### D6. 순서: 단일 도메인 서빙(11A~11C) → PlayerActor Workflow 기반(11E) → Room/Battle 프레임 공개(11D) → 전환 → 게이트 → 제거

11D와 11E의 실행 순서를 교체한다. `RoomJoin` 결과가 `RoomActor`에서 직접 클라이언트로 전달되면 `PlayerActor`가
입장 성공 여부를 모르게 되고, `BattleStart`에는 요청자 정보가 없어 `PlayerActor`의 `InRoom(room)` 검증 없이
직접 라우팅하면 타 방 전투를 시작할 수 있으며, `RoomLeave`는 0바이트 payload여서 `PlayerActor`가 authoritative
room 상태를 갖지 않으면 대상을 결정할 수 없다.

따라서 임시/낙관적 `_current_room` 상태를 두지 않고, **11E(PlayerActor workflow 및 Zone/Room 응답 채널)를
먼저 구축한 뒤 11D(Room/Battle 외부 프레임 공개)를 진행**한다.
- 모든 Room/Battle 외부 요청은 반드시 `PlayerActor`를 통과한다.
- 클라이언트가 보낸 room id를 검증 없이 `RoomActorKey`로 사용하지 않는다.
- 11D와 11E가 모두 끝나 테스트 증거가 확보될 때까지 Room/Battle 경로는 완료로 표시하지 않는다.

## 서브 스텝

### Phase 1 — 단일 도메인 서빙 격차 (production 미변경)

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 11A | `feat(adapter): bind sessions and authenticate on the worker path` | 신규 `tests/worker_session_test.cpp` |
| 11B | `feat(adapter): route player commands to the owning PlayerActor` | Purchase / EquipSkill |
| 11C | `feat(adapter): route zone commands and AOI results` | EnterZone / Move / LeaveZone |

### Phase 2 — Workflow와 Room/Battle 라우팅

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 11E | `feat(adapter): own room entry and return in the PlayerActor workflow state` | room 계약 §3·§4, Zone→Player 응답 채널 기반 (a) location R1/R3 복원, (b) tell 실패 롤백·실패 프레임 |
| 11D | `feat(adapter): route room and battle commands through the PlayerActor` | RoomJoin / BattleStart / UseSkill / SetMoveIntent / RoomLeave |
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

의존: 11A → 11B → 11C → 11E → 11D → 11F → 11G → 11H → 11I → 11J → 11K.

## 진행 상황

| 스텝 | 상태 | 비고 |
| --- | --- | --- |
| 11A | **완료** | 아래 "11A 결과" 참고 |
| 11B | **완료** | 아래 "11B 결과" 참고 |
| 11C | **완료** | 아래 "11C 결과" 참고 |
| 11E | 진행 중 | PlayerActor workflow 및 응답 채널 (11D 선행) |
| 11D, 11F~11K | 미착수 | |

### 11A 결과

- sink가 session identity의 connection → player를 소유하고, PlayerActor가 `ConnectionRef`를 보유한다.
- legacy `PlayerAttachResult` 6개 결과값에 각각 owner를 배정하고 `worker_session_test.cpp`로 고정했다.
- session entry는 connection id로 키를 잡고 generation을 함께 검증한다. close 통지를 놓친 stale entry가
  재사용된 slot에 이전 player의 세션을 넘기지 않고 fail closed된다.
- `RequestSink`에 "Worker마다 1 인스턴스" 계약을 명시하고 sink가 owner thread를 debug assertion으로 고정했다.
- 관측용 live session count만 atomic mirror다. map 자체는 다른 thread에서 읽지 않는다. 이 규칙을 어긴
  **테스트 자신이 TSan data race로 잡혔고**, 제품 코드를 우회하지 않고 테스트가 계약을 따르도록 고쳤다.

#### 의도적 parity 변경 2건 (승인 2026-09-03)

**Pre-auth `Ping`은 state-free liveness operation으로 정의한다.** sink가 직접 `Pong`으로 답하고 ActorSlot,
DB activation, session state를 하나도 만들지 않는다. `Ping`은 gameplay command가 아니라 connection/liveness
protocol이기 때문이다.

**Legacy의 pre-auth provisional activity는 architecture artifact로 판단하여 parity 대상에서 제외한다.**
legacy는 인증 전 `Ping`을 connection 키 provisional actor로 보냈고, 그 활동이 이후 그 연결의 인증을 막았다.
이는 gameplay 계약이 아니라 옛 ActorRuntime 구조가 외부 동작으로 새어 나온 것이며, 보존하려면 새 구조에
불필요한 상태를 다시 만들어야 한다.

인증 경계는 그대로 유지한다.

```text
Unauthenticated
  Ping         → Pong, actor 생성 없음, session entry 없음
  Authenticate → 정상 인증
  그 외 전부    → Invalid, 연결 종료
```

`worker_session_test.cpp`가 고정하는 것:

| 테스트 | 고정 내용 |
| --- | --- |
| `test_pre_auth_ping_is_answered_without_an_actor` | Pong 수신, `actor_turns == 0`, session entry 없음 |
| `test_pre_auth_ping_does_not_block_authentication` | Ping → Pong → Authenticate 성공, 전체 흐름에서 `actor_turns == 1` (Ping은 turn을 만들지 않았다) |
| `test_only_ping_and_authenticate_cross_the_pre_auth_boundary` | 나머지 client frame 10종 전부 인증 전 거절. **11B 이후 라우팅이 추가돼도 계속 성립해야 한다** |

### 11B 결과

- `Purchase`(12바이트: 8바이트 idempotency key + 4바이트 product id)와 `EquipSkill`(4바이트 skill id)을
  legacy dispatcher와 같은 wire form으로 디코딩해 세션의 player actor로 라우팅한다. 둘 다 non-zero 검사까지
  동일하다.
- 도메인 실패는 D4대로 `Accepted` + 응답 프레임이다. `SkillNotOwned`가 응답으로 돌아오는 것을 테스트가
  확인한다. `Rejected`는 mailbox full 같은 overload에만 쓴다.
- 라우팅이 실제 도메인에 닿았는지는 **같은 idempotency key 재전송**으로 증명한다. 두 번째 응답이
  `replayed = 1`이고 잔액이 두 번 줄지 않았다는 것은 두 프레임이 같은 Player 상태를 봤다는 뜻이다.

#### 구현 중 발견한 격차 (수정함)

`PlayerActor`가 **bound connection이 닫힌 것을 알 방법이 없었다.** 그래서 한 번 접속한 player는 연결이
끊긴 뒤 재접속하면 자기 자신과 PlayerConflict가 나서 영구히 로그인할 수 없었다. legacy는
`PlayerSessionDirectory`가 close 시 양방향 entry를 지웠고 통합 테스트
`test_authenticates_one_session_and_allows_reconnect_after_passivation`이 이를 덮고 있었다.

수정: sink가 `onConnectionClosed`에서 `PlayerConnectionClosedMessage`를 player actor에게 보내고, actor는
`ConnectionRef`가 정확히 일치할 때만 binding을 해제한다(generation까지 비교하므로 이전 incarnation의 close
통지가 현재 세션을 끊지 못한다). `test_the_same_player_can_reconnect_after_disconnecting`이 고정한다.

이 release가 turn을 하나 만들기 때문에, 정확한 turn 수를 검사하는 테스트는 먼저 연결을 끊고 session release가
끝나기를 기다린 뒤에 검사한다. shutdown 순서에 의존하지 않게 하려는 것이다.

### 11C 결과

- `EnterZone`(16바이트: 8바이트 zone id + 4바이트 x + 4바이트 y), `Move`(8바이트: 4바이트 x + 4바이트 y),
  `LeaveZone`(0바이트)을 디코딩하여 세션의 `PlayerActor`로 라우팅한다.
- `RouteCoordinator`가 가졌던 per-player route state(`_current_zone`, `_route_epoch`)를 `PlayerActorAdapter`로
  옮겨, `PlayerActor`가 epoch을 관리하고 대상 `ZoneActor`로 `ZoneCommandMessage`를 tell한다.
- `ZoneActorAdapter` 및 `to_effects` zone overload를 통해 client로 27바이트 고정 규격의 유니캐스트 응답이 전달된다.
- legacy 조사 결과와 일치하게 **broadcast가 없음을 검증했다**: 두 번째 플레이어의 진입 시 첫 번째 플레이어에게
  unsolicited 프레임이 전달되지 않으며, AOI는 응답자의 `visible_players` 필드로만 전달된다.
- zone-to-zone 진입 시도 시 `TransferFailed`(5)를 반환하고 연결을 유지하며, 동일 zone 재입장은 epoch 불변 상태로
  전달되어 `AlreadyPresent`(1)와 기존 위치를 반환한다.
- zone id 0 및 zone 없는 Move/Leave는 `PlayerActor`에서 `CloseConnectionEffect`로 안전하게 거절하여 worker의
  throw(`invariant_violations == 0`)를 방지한다.
- 연결 종료 시 `PlayerConnectionClosedMessage`에서 `_current_zone`이 있으면 암묵적 `LeaveZoneCommand`를
  전달하여 Zone participant에서 제거됨을 후속 Move의 AOI `visible_count == 0`으로 고정했다.
- 테스트 결과: Debug 17개 중 13 PASS / 4 SKIP (MySQL), TSan worker 비-MySQL 8개를 5회 연속 PASS / 3 SKIP (MySQL), ASan-UBSan worker 8 PASS / 3 SKIP (MySQL). MySQL 실측은 계획대로 11I에서 수행.

#### 알려진 격차 (11E에서 수정)

1. **`TellActorEffect` 실패 시 route state 롤백 미처리**
   `applyEffectBatch`는 tell 실패 시 `effect_tell_failures`만 올리고(`worker.cpp:1759`), effect는 turn이 반환된
   뒤 적용되므로 adapter가 실패를 관측할 수 없다. 반면 route state(`_route_epoch`, `_current_zone`)는 tell 전에
   커밋된다.
   Zone mailbox / remote inbox / actor table 포화 시:
   - `EnterZone`: `_route_epoch += 1`, `_current_zone = zone`이 커밋되지만 `TellActorEffect`가 조용히 실패하면
     Player는 들어간 적 없는 zone에 있다고 믿고, client는 응답을 받지 못한다.
   - `LeaveZone`: `_current_zone.reset()`이 커밋되지만 `TellActorEffect`가 조용히 실패하면 Zone에 participant가
     남는다 (재입장 시 epoch이 더 커서 re-seat되므로 피해는 작다).
   legacy는 이 지점에서 명시적으로 롤백했다(`rollbackEnter`, `protocol_gateway.cpp:289`).

2. **Location R1 / R3 미반영**
   - **R1 (재입장 위치 복원, `protocol_gateway.cpp:255`)**: 플레이어가 연결 해제 후 동일 Zone에 재입장할 때
     클라이언트가 보낸 임의 좌표 대신 서버에 저장된 마지막 유효 위치(`last_location.position`)를 복원하는 규칙.
   - **R3 (영속 복귀지점, `room-entry-handoff-contract.md:49`)**: PlayerActor가 dirty 상태를 DB에 플러시할 때
     최근 머문 Zone/좌표가 `PlayerRecord.last_location`으로 영속화되어, 액터 패시베이션 후 활성화 시 복구되는 규칙.
   - **원인**: 11C 현재 `ZoneResult`는 ZoneActor에서 클라이언트로 직접 unicast되므로 PlayerActor를 거치지 않는다.
     따라서 `Player::last_location` 갱신, DB 영속화, 재활성화 복원이 동작하지 않는다.

**11E 해결 방안 및 검증 기준**:
adapter가 실패를 관측하고 location을 갱신하려면 **Zone→Player 응답 채널**이 필수적이다. 이 채널은 11E가
transition correlation을 위해 이미 만들 예정이므로, 11E에서 한 번에 만들어 다음을 함께 닫는다:
- (a) Zone 결과 수신 시 `Player::last_location` 갱신 및 DB 영속화 (`test_player_persists_zone_location_on_save`)
- (b) 재접속 후 재입장 시 이전 위치 복원 (`test_reconnect_restores_last_zone_position`)
- (c) Zone tell 실패 시 timer 기반 timeout 검출 후 route state 롤백 및 에러 응답 프레임 발송
과부하 및 위치 복원 외에 11C의 정상 경로는 정확하므로 부채로 남기며, 11I 게이트 재실행에서 과부하 주입 시
관측될 수 있다.

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
  기반이다. 신규 경로의 대응물이 무엇인지(또는 불필요한지)를 11E에서 먼저 결론낸다. 또한 11C에서 기록된
  `TellActorEffect` 실패 롤백 미처리 격차 역시 11E의 응답 채널에서 함께 해결한다 (11I 게이트 과부하 시 관측 가능).
- **D4 함정.** 도메인 실패를 잘못 매핑하면 부하 중 연결이 끊기고 parity 실패로 나타난다.
- **11I의 MySQL 실측이 처음이다.** `mysql_close()` boundedness를 포함해 새로 드러날 문제가 있을 수 있다.
- **11J 후 `snf::server::Player` 이름과 `snf_game` 위치의 불일치가 남는다.** 네임스페이스 정리는 범위 밖이다.

## 범위 밖

- `snf::server::Player` → `snf::game::Player` 네임스페이스 정리
- durable workflow system (§12 규칙 4)
- actor live migration, dynamic worker scaling, idle passivation
- matchmaking, group 동시 입장, 프로세스 간 이동, mid-battle reconnect (계약 §1의 명시적 범위 밖)
