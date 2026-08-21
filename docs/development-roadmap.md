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
- Room `Waiting → Running → Cleared`, 자기 timer로 끝나는 placeholder 전투, clear 시 참가자
  보상 tell과 passivation
- Room 입장·시작 요청과, 요청 없이 나가는 clear 알림 (`request_id = 0`)

### 빌드 경계

- `snf_game`: 도메인 상태 기계와 값. runtime·net·protocol·MySQL·threads를 링크하지 않는다
- `snf_mysql_player_repository`: MySQL을 아는 유일한 target
- `snf_game_tests`(링크)와 `snf_game_layer`(include 검사)가 경계를 강제한다

## 현재 정리 기준

- Repository는 Player snapshot의 `load/save`만 담당한다. gameplay 판정은 PlayerActor에서 한다.
- Runtime, network와 콘텐츠는 typed command/result로만 연결한다.
- 구현되지 않은 executor, network backend와 분산 구조는 문서에도 선행 설계하지 않는다.
- 기능 수보다 하나의 콘텐츠가 정상·포화·disconnect·shutdown까지 종결되는지를 우선한다.

## 다음 콘텐츠: 협동 Battle Room

Party가 입장하는 작은 협동 보스 인스턴스다. MMORPG 월드 기능을 넓히지 않고, Actor 상태
소유권이 공유 콘텐츠에서 주는 장점과 비용을 보여주는 것이 목적이다.

> **진행 중.** 상태 기계와 보상 경로가 placeholder 전투(5초 후 무조건 clear)로 동작하고,
> 클라이언트가 실제 TCP로 입장·시작하고 clear 알림을 받는다. damage가 없어 아래 완료 조건
> 중 request sequence와 관련된 항목은 아직 검증할 대상 자체가 없다.

### 상태와 명령

```text
Waiting → Running → Cleared
                  └→ Failed
           ↓
        Closing → Passivated
```

- `Join`, `Ready`, `UseSkill`, `Tick`, `Disconnected`, `Reconnected`, `Leave`
- Room이 participant combat snapshot, boss HP/phase, cooldown과 request sequence를 소유
- PlayerActor는 영속 economy와 session을 계속 소유
- client는 damage 값을 보내지 않고 skill ID만 보낸다

### 완료 조건

- 같은 Room 명령이 FIFO로 결정적으로 적용되고 handler 동시 실행이 없다. (충족)
- 중복 request sequence가 damage나 clear를 두 번 적용하지 않는다.
- stale connection generation이 이전 Player를 조작하지 못한다.
- disconnect/reconnect와 timeout 정책이 명시돼 있다.
- clear/fail 결과는 한 번만 생성되고 Room은 timer와 mailbox를 정리한 뒤 passivate된다. (충족)
- 여러 Room 분산 부하와 하나의 hot Room 부하를 비교한다.
- queue 포화, shutdown 중 tick과 late completion 테스트를 포함한다.
- Debug, TCP integration, ASan·UBSan과 TSan을 통과한다. (충족)

### 비범위

- 대규모 seamless world와 process 간 migration
- matchmaking service
- 복잡한 전투 수치와 클라이언트 표현
- 새 persistence 모델이나 별도 projection
- Runtime 통합, io_uring 또는 Actor 내부 병렬화

## 콘텐츠 완료 후 포트폴리오 산출물

1. 요구사항 → 상태 → command/result → 예외 흐름을 담은 콘텐츠 계약
2. Actor 소유권과 async/backpressure/lifecycle 결정만 담은 아키텍처 문서
3. 정상·hot Actor·포화·shutdown 부하 결과
4. 5분 안에 재현 가능한 TCP demo와 실행 명령
