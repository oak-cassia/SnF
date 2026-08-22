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
- in-memory 및 bounded MySQL load/save adapter
- street 누적 경험치 영속화와, 그로부터 파생되는 레벨·공격/체력

### Shared state

- Zone enter/move/leave, periodic tick, AOI와 빈 Actor passivation
- route epoch과 failure-safe cross-zone handoff
- Party membership, capacity, stale leave 차단과 passivation
- Room `Waiting → Running → Cleared | Failed`. 100ms tick이 wave/minion/boss spawn과 ordered
  `BattleDigest`를 진행하고, client의 skill id를 첫 생존 적에게 즉시 적용한다. damage, cooldown과
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
  아래 구현 순서 4의 battle outcome commit이 이 기준을 넓히는 첫 변경이다.
- Runtime, network와 콘텐츠는 typed command/result로만 연결한다.
- 구현되지 않은 executor, network backend와 분산 구조는 문서에도 선행 설계하지 않는다.
- 기능 수보다 하나의 콘텐츠가 정상·포화·disconnect·shutdown까지 종결되는지를 우선한다.

## 다음 콘텐츠: 4인 협동 Wave Battle

Party가 입장하는 작은 협동 인스턴스다. MMORPG 월드 기능을 넓히지 않고, Actor 상태 소유권이
공유 콘텐츠에서 주는 장점과 비용을 보여주는 것이 목적이다. 전투는 위치 없는 wave 전투다.
위치·이동·충돌을 빼면 "client는 skill ID만 보내고 서버가 damage를 판정한다"는 성질이 1/3
비용으로 남는다.

> **진행 중 — 구현 순서 2a 완료.** 클라이언트가 실제 TCP로 Zone에서 입장해 첫 wave와 minion 처치,
> tick에서 boss 등장, clear와 원래 Zone 복귀까지 `BattleDigest`로 관찰한다. 입장·복귀 saga와 그 보상은
> `docs/room-entry-handoff-contract.md`에 기록돼 있다. 적 공격, 참가자 HP/사망과 threat는 2b 범위다.

### 상태와 명령

```text
Waiting → Running → Cleared
                  └→ Failed
           ↓
        Closing → Passivated
```

- `JoinRoom`, `LeaveRoom`, `StartBattle`, `UseSkill`, `RoomSimulationTick`, `BattleDeadline`
- Room이 participant combat snapshot, enemy HP와 spawn 순서, boss phase, skill cooldown과 request
  sequence를 소유한다. participant 생사와 enemy attack/threat는 2b에서 추가한다
- PlayerActor는 영속 progression과 session을 계속 소유
- client는 damage 값을 보내지 않고 skill ID만 보낸다

### 구현 순서

1. **최소 전투** — `UseSkill`, boss HP, deadline 기반 cooldown, request sequence 중복 방어,
   `Cleared`/`Failed`, 참가자 fanout. enemy도 tick도 없다. (완료)
2. **Wave simulation**
   - **2a — Wave와 관찰 경계:** 100ms tick, minion/boss spawn, 첫 생존 적 targeting, 즉시 cast와
     ordered `BattleDigest`, hard deadline, tick 예산 측정. (완료)
   - **2b — 적 행동과 생사:** enemy attack/cooldown/threat, participant HP/death와 결정적 tie-break
3. **Session 안정성** — connection generation으로 stale 명령 거부
4. **결과 영속화** — `BattleResult`, `battle_id` idempotency, reward/progression transaction.
   여기서 persistence 범위를 넓힌다. 상세 계약은 이 단계 직전에 확정한다
5. **Progression** — skill unlock과 loadout, CombatSnapshot으로 전달
6. **부하** — hot Room 하나 vs 분산 Room N개, outbound 포화, shutdown과 late completion

각 단계는 wire에서 관찰 가능한 상태로 끝난다. tick과 브로드캐스트를 한 단계로 묶은 이유가
그것이다. 내부에서 몬스터가 spawn하고 공격하는데 client가 알 수 없으면 그 단계는 완결된
vertical slice가 아니다.

### 확정한 계약

- **시간은 deadline이다.** cooldown은 tick마다 감소시키지 않고 `ready_at`으로 둔다.
  `ActorContext::observedAt()`이 turn 시작 시각을 주므로 Room은 clock 없이 남고, Step 1이 tick
  없이 성립한다. 전투의 절대 deadline은 `UseSkill`, tick과 deadline timer 모두가 확인한다
- **cast 적용과 관찰은 분리한다.** `UseSkill` turn에서 targeting·damage·cooldown·sequence를 즉시
  적용하고 요청자에게 `SkillAcknowledged`를 보낸다. 발생 이벤트는 ordered buffer에 쌓여 tick 또는
  threshold/terminal 경계에서 `BattleDigest`로 전 참가자에게 fanout된다
- **한 command의 인과 그룹은 갈라지지 않는다.** Damage 뒤 선택적인 Died를 전부 buffer에 추가한
  다음 threshold를 검사한다. event를 drop하지 않으며 digest sequence는 실제 방출 때만 증가한다
- **tick은 one-shot 사슬이다.** binding이 deadline을 먼저 예약한 다음 `ExistingOnly` tick을 예약한다.
  tick 예약 거절은 metric으로 남기고 deadline을 backstop으로 쓰며, deadline 예약 거절은 Logic Runtime
  실패로 승격한다
- **outbound 포화는 연결을 닫고 Room은 계속 진행한다.** Room을 suspend시키면 느린 client 하나가
  4인 전투를 멈춘다. 살아 있는 연결이 이벤트 일부를 잃는 경로가 없으므로 resync snapshot은
  만들지 않는다. 부하 측정에서 이 정책이 실제 문제로 확인되면 그때 설계한다
- **죽음과 퇴장은 다른 상태다.** 이 계약은 2b에서 생사 모델과 함께 구현한다. 현재 leave와
  disconnect만 participant에서 제거되고 보상을 포기하며, clear는 남아 있는 participant에게 지급한다
- **시작은 명시적 `StartBattle`이다.** Room은 현재 참가자 수와 상한만 알고 원래 파티가 몇 명인지
  모른다. 자동 시작은 Party roster를 Room까지 넘기는 별개 작업이므로 이 콘텐츠에 넣지 않는다

### 완료 조건

- 같은 Room 명령이 FIFO로 결정적으로 적용되고 handler 동시 실행이 없다. (충족)
- 중복 request sequence가 damage나 clear를 두 번 적용하지 않는다. (충족)
- stale connection generation이 이전 Player를 조작하지 못한다.
- 같은 `battle_id`의 결과가 두 번 도착해도 보상이 두 번 지급되지 않는다.
- 같은 입력에서 같은 `BattleDigest` 이벤트 순서가 나온다. (충족)
- disconnect/reconnect와 timeout 정책이 명시돼 있다. (충족: 입장 handoff 계약 §6. disconnect는
  좌석을 해제하고 보상을 포기하며, Room 재적을 영속화하지 않으므로 reconnect는 저장된 Zone으로
  복원된다. mid-battle reconnect는 명시적 비범위다)
- clear/fail 결과는 한 번만 생성되고 Room은 timer와 mailbox를 정리한 뒤 passivate된다. (충족)
- 여러 Room 분산 부하와 하나의 hot Room 부하를 비교한다.
- 느린 client가 outbound를 포화시켰을 때 전투가 계속 진행된다.
- deadline/Tick 예약 포화와 terminal 뒤 stale timer 정리를 포함한다. (충족)
- Debug, TCP integration, ASan·UBSan과 TSan을 통과한다. (충족)

세 계층의 중복 방어를 하나의 콘텐츠에서 보여주는 것이 이 목록의 핵심이다.

```text
request_sequence       → transport/요청 중복
connection generation  → session 수명을 넘긴 stale 명령
battle_id              → 영속 연산 중복
```

### 비범위

- 위치, 이동, projectile, 충돌, pathfinding
- `Ready`와 countdown, 그리고 자동 시작을 위한 Party roster 전달
- 진행 중인 전투로의 reconnect와 주기적 resync snapshot
- 대규모 seamless world와 process 간 migration
- matchmaking service
- 복잡한 전투 수치와 클라이언트 표현
- 별도 projection이나 read model. battle outcome commit은 구현 순서 4에서 예외로 들어온다
- Runtime 통합, io_uring 또는 Actor 내부 병렬화

## 콘텐츠 완료 후 포트폴리오 산출물

1. 요구사항 → 상태 → command/result → 예외 흐름을 담은 콘텐츠 계약
2. Actor 소유권과 async/backpressure/lifecycle 결정만 담은 아키텍처 문서
3. 정상·hot Actor·포화·shutdown 부하 결과
4. 5분 안에 재현 가능한 TCP demo와 실행 명령
