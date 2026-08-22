# SnF 개발 로드맵

> 이 문서는 현재 완료 범위와 바로 다음 콘텐츠만 기록한다. 과거 Phase별 구현 과정은 Git
> history가 보존하며, 측정되지 않은 Runtime 최적화는 로드맵에 미리 추가하지 않는다.

## 완료된 기반

### Network와 protocol

- level-triggered `epoll` reactor와 non-blocking TCP
- 길이 기반 binary frame, partial receive/send와 protocol validation
- connection generation을 통한 stale outbound 차단
- Session별 pending byte 상한과 bounded outbound channel
- 실제 TCP 통합 테스트와 non-blocking load client

### Actor Runtime

- `ActorKey{Kind, EntityId}` 고정 shard와 Actor별 FIFO mailbox
- Worker turn budget과 mailbox-safe passivation
- lazy C++20 coroutine handler와 owning-Worker 전용 resume/destroy
- bounded in-flight/continuation reservation
- completion, cancel, late completion과 shutdown 경합 처리
- queue wait, suspension, depth와 high-water metric
- Worker 소유 일회성 timer와 incarnation 기반 stale 폐기
- Actor 간 비동기 tell과 대상 Binding의 submission 조립

### Player

- provisional authentication에서 persistent Player route로 전환
- PlayerActor의 Session/Economy 단일 소유권
- Actor 수명 범위 idempotent NPC 구매
- dirty snapshot 제출, Player별 coalescing과 save 직렬화
- disconnect final save와 reconnect 복원
- connection generation 기반 one-live-session과 exact-match passivation
- in-memory 및 bounded MySQL load/save adapter
- street 누적 경험치 영속화와, 그로부터 파생되는 레벨·공격/체력

### Shared state

- Zone enter/move/leave, periodic tick, AOI와 빈 Actor passivation
- route epoch과 failure-safe cross-zone handoff
- Party membership, capacity, stale leave 차단과 passivation
- Room `Waiting → Running → Cleared | Failed`. 100ms tick이 정수 좌표 이동, wave/minion/boss spawn,
  가장 가까운 생존 대상 추격·공격과 ordered `BattleDigest`를 진행한다. damage, HP, cooldown과
  중복 판정은 서버가 소유하며 clear 시 참가자 보상 tell, 종결 뒤 passivation한다
- Zone과 Room 사이의 보상 있는 입장·복귀 handoff. 전투 중 Player는 Zone에 없고, clear·leave·
  disconnect가 원래 Zone의 원래 좌표로 되돌린다
- Room 입장·시작·퇴장 요청, 2바이트 skill ack와 요청 없이 나가는 digest·clear·복귀 알림
  (`request_id = 0`)

### 빌드 경계

- `snf_game`: 도메인 상태 기계와 값. runtime·net·protocol·MySQL·threads를 링크하지 않는다
- `snf_mysql_player_repository`: MySQL을 아는 유일한 target
- `snf_game_tests`(링크)와 `snf_game_layer`(include 검사)가 경계를 강제한다

## 현재 정리 기준

- Repository는 Player snapshot의 `load/save`만 담당한다. gameplay 판정은 PlayerActor에서 한다.
  구현 순서 4도 이 기준을 넓히지 않는다. 보상은 새 저장 경로 없이 기존 snapshot 경로로만 내려간다.
- Runtime, network와 콘텐츠는 typed command/result로만 연결한다.
- 구현되지 않은 executor, network backend와 분산 구조는 문서에도 선행 설계하지 않는다.
- 기능 수보다 하나의 콘텐츠가 정상·포화·disconnect·shutdown까지 종결되는지를 우선한다.

## 다음 콘텐츠: 4인 협동 Wave Battle

Party가 입장하는 작은 협동 인스턴스다. MMORPG 월드 기능을 넓히지 않고, Actor 상태 소유권이
공유 콘텐츠에서 주는 장점과 비용을 보여주는 것이 목적이다. 처음에는 위치 없는 wave 전투로
계획했지만 2a가 tick, ordered digest, fanout, payload 상한과 비용 metric을 먼저 완성한 뒤 범위를
바꿨다. 정수 좌표는 threat table 없이 가장 가까운 생존 대상이라는 관찰 가능한 targeting 근거를
주고, 빠르게 변하는 좌표와 HP를 Room Actor 하나가 직렬화하는 비용도 드러낸다. 대신 직선 이동만
허용하며 충돌·장애물·pathfinding·projectile·Room AOI·resync snapshot은 계속 만들지 않는다.

> **구현 순서 2b 완료.** 실제 TCP에서 이동, 적 추격·피해, boss clear와 `PartyDefeated` 실패가
> `BattleDigest`로 관찰되고 두 종결 모두 원래 Zone 좌표로 복귀한다. 입장·복귀 saga와 그 보상은
> `docs/room-entry-handoff-contract.md`에 기록돼 있다.

### 상태와 명령

```text
Waiting → Running → Cleared
                  └→ Failed
           ↓
        Closing → Passivated
```

- `JoinRoom`, `LeaveRoom`, `StartBattle`, `UseSkill`, `SetMoveIntent`, `RoomSimulationTick`, `BattleDeadline`
- Room이 participant combat snapshot·현재 HP·좌표·이동 의도, enemy HP·좌표·spawn 순서·공격
  cooldown, boss phase와 skill/movement request sequence를 소유한다
- PlayerActor는 영속 progression과 session을 계속 소유
- client는 damage 값을 보내지 않고 skill ID만 보낸다

### 구현 순서

1. **최소 전투** — `UseSkill`, boss HP, deadline 기반 cooldown, request sequence 중복 방어,
   `Cleared`/`Failed`, 참가자 fanout. enemy도 tick도 없다. (완료)
2. **Wave simulation**
   - **2a — Wave와 관찰 경계:** 100ms tick, minion/boss spawn, 첫 생존 적 targeting, 즉시 cast와
     ordered `BattleDigest`, hard deadline, tick 예산 측정. (완료)
   - **2b — Minimal Arena와 생사:** 8방향 persistent movement, nearest-live targeting, enemy
     attack/cooldown, participant HP/death와 결정적 ID tie-break. (완료)
3. **Session 안정성** — generation 기반 admission/routing 방어, Closing 동안 reconnect 차단과
   Player·Connection exact-match passivation. Actor 내부에 중복 generation guard를 두지 않는다. (완료)
4. **보상 인계 책임 전달** — tell 수락과 최초 load 성공 뒤의 책임 이전, 스냅샷 큐 수락까지
   PlayerActor 상주, tell·load·큐 거절 계측. 프로세스 장애를 넘는 보상 복구는 보장하지 않는다
5. **Room 부하 실측** — hot Room 하나와 분산 Room N개 비교, 느린 client의 outbound 포화 격리.
   shutdown은 시나리오 종료 smoke로만 확인하고 별도 부하 축으로 만들지 않는다

각 단계는 wire에서 관찰 가능한 상태로 끝난다. tick과 브로드캐스트를 한 단계로 묶은 이유가
그것이다. 내부에서 몬스터가 spawn하고 공격하는데 client가 알 수 없으면 그 단계는 완결된
vertical slice가 아니다.

### Step 4 보상 인계 계약

현재 보상 전달에는 아무도 책임지지 않는 지점이 세 곳이다. reward tell이 거절되면 Room은 반환값을
버리고 passivate한다. offline Player를 tell로 활성화한 뒤 최초 record load가 실패하면
`pending_grant`를 지운다. load가 성공해 전달을 인수해도 `publishDirtySnapshot`의 큐 admission이
거절되면 dirty mask를 되돌린 직후 `PassivateIfIdle`로 떠나므로 그 mask를 flush할 주체가 함께
사라진다. await하는 최종 저장은 연결 close 경로에만 있다. Step 4는 durable grant 테이블 없이 큐
거절 뒤의 구멍을 닫고 tell·load 실패를 계측된 허용 유실로 확정한다.

- 일반 전투 보상은 tell이 수락되고 최초 record load가 성공한 순간 PlayerActor가 책임을 인수한다.
  PlayerActor는 보상을 메모리에 반영하고 스냅샷이 저장 큐에 수락될 때까지 상주한다
- 큐 수락 이후에는 `PlayerPersistenceService`가 프로세스 생존 범위에서 저장을 재시도하므로
  PlayerActor는 저장 성공을 기다리지 않는다. 저장 성공까지 붙잡으면 DB 장애 동안 clear한 방마다
  actor가 상주로 누적되고, 보호하려던 용량이 먼저 무너진다
- 큐 수락 재시도는 tick과 같은 방식이다. `trySchedule`로 one-shot timer를 걸고 `ExistingOnly`로
  자기에게 돌아온다. 새 mechanism을 만들지 않는다. tick과 달리 backstop이 없으므로 timer 예약이
  거절되면 그 보상은 아래 허용 유실로 떨어지고 같은 카운터에 남는다
- tell 거절, 최초 load 실패와 저장 큐 거절은 각각 계측한다. `_tick_schedule_rejections`와 같은
  방식이며, 유실을 허용한다는 정책과 유실을 관측하지 못하는 상태를 구분하는 장치다
- tell 거절, 최초 load 실패와 프로세스 장애로 인한 보상 유실은 허용한다. mailbox는 actor별이 아니라 worker당
  `queue_capacity_per_worker`(현재 4096)이고 clear 하나가 만드는 tell은 최대 참가자 수다. 거절되려면
  그 worker 큐가 이미 포화여야 하며, 그 상태에서 무너지는 것은 보상 하나가 아니다
- Room은 terminal 정리와 passivation을 유지하고 재전달 timer를 갖지 않는다. 프로세스가 살아 있어도
  Room에 재시도를 얹을 자리가 없다. 같은 turn 안의 반복은 대상 mailbox를 비워줄 주체를 돌리지
  못하고, timer는 이미 충족된 terminal passivation 조건을 무른다
- 고가치 보상은 durable 원장과 멱등 키로 별도 처리한다. 진행 중 전투 자체가 프로세스 장애로
  사라지는 세션형 서버에서 일반 경험치만 완전 복구하는 것은 비대칭이다

Step 4에서는 durable grant 테이블, 여러 Player progression의 all-or-nothing transaction, 범용
transaction framework, 범용 saga/outbox, 과거 전체 `Transaction` abstraction 복원, 새 client
protocol과 reward 적용 순서 보장을 만들지 않는다. 전달이 at-most-once이고 저장이 record 전체
덮어쓰기이므로 이 범위에는 멱등 키가 들어갈 자리가 없다.

### 확정한 계약

- **시간은 deadline이다.** cooldown은 tick마다 감소시키지 않고 `ready_at`으로 둔다.
  `ActorContext::observedAt()`이 turn 시작 시각을 주므로 Room은 clock 없이 남고, Step 1이 tick
  없이 성립한다. 전투의 절대 deadline은 `UseSkill`, tick과 deadline timer 모두가 확인한다
- **cast 적용과 관찰은 분리한다.** `UseSkill` turn에서 targeting·damage·cooldown·sequence를 즉시
  적용하고 요청자에게 `SkillAcknowledged`를 보낸다. 발생 이벤트는 ordered buffer에 쌓여 tick 또는
  threshold/terminal 경계에서 `BattleDigest`로 전 참가자에게 fanout된다
- **한 command의 인과 그룹은 갈라지지 않는다.** Damage 뒤 선택적인 Died를 전부 buffer에 추가한
  다음 threshold를 검사한다. event를 drop하지 않으며 digest sequence는 실제 방출 때만 증가한다
- **좌표는 Room 안에서만 유효하다.** `SetMoveIntent`는 의도만 저장하고 다음 tick이 생존 참가자를
  움직인다. skill은 현재 좌표에서 사거리 안의 가장 가까운 생존 적을, 적은 가장 가까운 생존
  참가자를 고르며 동률은 작은 ID다. Room 종료 뒤에는 입장 전 Zone 좌표로 복귀한다
- **적 행동은 적별로 인터리브한다.** EnemyId 순서로 대상 선택 → 이동 → 선택적 피해 → 선택적
  사망을 끝낸 다음 다음 적이 살아 있는 대상을 다시 고른다. 마지막 생존 참가자가 죽으면
  `PartyDefeated`로 즉시 종결하며 deadline 실패와 wire reason을 구분한다
- **tick은 one-shot 사슬이다.** binding이 deadline을 먼저 예약한 다음 `ExistingOnly` tick을 예약한다.
  tick 예약 거절은 metric으로 남기고 deadline을 backstop으로 쓰며, deadline 예약 거절은 Logic Runtime
  실패로 승격한다
- **outbound 포화는 연결을 닫고 Room은 계속 진행한다.** Room을 suspend시키면 느린 client 하나가
  4인 전투를 멈춘다. 살아 있는 연결이 이벤트 일부를 잃는 경로가 없으므로 resync snapshot은
  만들지 않는다. 부하 측정에서 이 정책이 실제 문제로 확인되면 그때 설계한다
- **죽음과 퇴장은 다른 상태다.** 죽은 participant는 audience와 clear 보상 대상에 남아 관전하지만
  이동·cast·target 대상에서는 빠진다. leave와 disconnect는 participant를 제거하고 보상을 포기하며
  `ParticipantLeft`가 남은 client의 유령 상태를 지운다
- **시작은 명시적 `StartBattle`이다.** Room은 현재 참가자 수와 상한만 알고 원래 파티가 몇 명인지
  모른다. 자동 시작은 Party roster를 Room까지 넘기는 별개 작업이므로 이 콘텐츠에 넣지 않는다

### 완료 조건

- 같은 Room 명령이 FIFO로 결정적으로 적용되고 handler 동시 실행이 없다. (충족)
- 중복 request sequence가 damage나 clear를 두 번 적용하지 않는다. (충족)
- stale connection generation이 admission/routing 경계에서 이전 Player를 조작하지 못하며, 이전
  connection의 늦은 deactivation이 새 Closing 세션을 제거하지 않는다. (충족)
- 보상 tell이 수락되고 최초 record load가 성공하면 스냅샷이 저장 큐에 수락될 때까지 PlayerActor가
  상주한다.
- tell 거절, 최초 load 실패와 저장 큐 거절이 각각 카운터로 남는다. 프로세스 장애를 넘는 보상 복구는
  완료 조건이 아니다.
- 같은 입력에서 같은 `BattleDigest` 이벤트 순서가 나온다. (충족)
- disconnect/reconnect와 timeout 정책이 명시돼 있다. (충족: 입장 handoff 계약 §6. disconnect는
  좌석을 해제하고 보상을 포기하며, Room 재적을 영속화하지 않으므로 reconnect는 저장된 Zone으로
  복원된다. mid-battle reconnect는 명시적 비범위다)
- clear/fail 결과는 한 번만 생성되고 Room은 timer와 mailbox를 정리한 뒤 passivate된다. (충족)
- 여러 Room 분산 부하와 하나의 hot Room 부하를 비교한다.
- 부하 리포트가 보상 tell 거절, 최초 load 실패와 저장 큐 거절 카운터 값을 포함한다. 카운터를 만드는 것은 구현
  순서 4이고, 포화 상황에서 그 값을 보고하는 것이 이 축이다.
- 느린 client가 outbound를 포화시키면 해당 연결만 종료되고 Room과 건강한 참가자는 계속 진행한다.
- deadline/Tick 예약 포화와 terminal 뒤 stale timer 정리를 포함한다. (충족)
- Debug, TCP integration, ASan·UBSan과 TSan을 통과한다. (충족)

두 계층의 중복 방어와 명시된 유실 경계를 하나의 콘텐츠에서 보여주는 것이 이 목록의 핵심이다.

```text
request_sequence       → transport/요청 중복
connection generation  → admission/routing에서 session 수명을 넘긴 stale 명령
계측된 허용 유실       → 프로세스 장애를 넘는 영속 보장의 명시적 비범위
```

### 비범위

- 충돌, 장애물, pathfinding, projectile, Room AOI와 진행 중 resync snapshot
- `Ready`와 countdown, 그리고 자동 시작을 위한 Party roster 전달
- 진행 중인 전투로의 reconnect와 주기적 resync snapshot
- 대규모 seamless world와 process 간 migration
- matchmaking service
- 복잡한 전투 수치와 클라이언트 표현
- skill unlock, loadout과 추가 skill 콘텐츠. 현재 단일 Slash 카탈로그를 유지한다
- 별도 projection, read model, 보상 원장 테이블. 구현 순서 4도 새 테이블을 만들지 않는다
- Runtime 통합, io_uring 또는 Actor 내부 병렬화

## 콘텐츠 완료 후 포트폴리오 산출물

1. 요구사항 → 상태 → command/result → 예외 흐름을 담은 콘텐츠 계약
2. Actor 소유권과 async/backpressure/lifecycle 결정만 담은 아키텍처 문서
3. 정상·hot Room·분산 Room과 outbound 포화 측정 결과
4. 5분 안에 재현 가능한 TCP demo와 실행 명령
