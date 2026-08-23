# Step 5 Room 부하 측정 리포트

측정일은 2026-08-23이다. `snf-server-dev:latest`의 Release build를 Docker Linux
6.12.5-linuxkit/aarch64에서 실행했다. container에 보인 자원은 10 CPU, 약 7.65 GiB memory이고
compiler는 GCC 13.3.0이다. 각 값은 6초 단일 실행의 용량 탐색 결과이므로 장기 benchmark의 신뢰
구간이 아니라 설계 경계와 다음 병목을 찾는 자료다.

## 방법

공통 workload는 `LoadScenario::Battle`, Room당 4명이다. 축 B는 client당 10 RPS로 고정하고 축 A만
목표 RPS를 바꿨다. 모든 client가
`Authenticate` → `EnterZone` → `RoomJoin`을 마친 뒤 방별 leader 하나가 `BattleStart`를 보내고,
참가자는 `UseSkill`과 `SetMoveIntent`를 번갈아 보낸다. Slash는 사거리 내 모든 생존 적을
EnemyId 순서로 타격한다. default 100ms Room tick과 이 전투 규칙을 사용했다. server는 5초
시계열과 종료 summary를, load client는 요청 RTT와 push frame 수·bytes,
digest 도착 간격을 기록했다.

`UseSkill`의 Slash cooldown은 1초다. 따라서 높은 목표 RPS에서는 번갈아 보내는 command의 절반인
`SetMoveIntent`는 적용되지만, 초당 1회를 넘는 `UseSkill` 대부분은 `SkillOnCooldown`으로 일찍
종결된다. 아래 response/s는 이 status 응답까지 포함한 Actor admission·dispatch·reply 처리량이며,
매 command가 targeting·damage·event 생성을 수행하는 전투 연산 처리량은 아니다.

대표 실행은 다음과 같다.

```bash
SNF_ACTOR_WORKER_COUNT=1 ./build/release/snf_server

./build/release/snf_load_client \
  --connections 1024 \
  --scenario battle \
  --players-per-room 4 \
  --duration 6 \
  --requests-per-second 10 \
  --connect-timeout-ms 15000 \
  --request-timeout-ms 15000
```

`tick_overruns`는 한 tick turn이 binding의 5ms 실행 budget을 넘은 횟수다. 100ms 주기 유지 여부는
이 값만으로 알 수 없으므로 actor timer lateness와 client의 digest interval도 함께 판정했다.
모든 sweep은 6초라 시작 즉시 생성되는 첫 minion wave 10마리만 포함한다. default 두 번째 wave는
20초, boss는 40초에 생성되므로 tick 분포는 opening state의 용량 탐색 결과이지 전투 전체 구간의
peak tick 비용이 아니다.

## 축 A — 단일 Room 직렬화

1 Room × 4 client에서 목표 RPS만 올렸다. RTT는 gameplay response, tick 값은 server의
`tick_turn_nanoseconds`다.

| 목표 RPS/client | 실효 response/s | gameplay RTT p50/p99 | tick turn p50/p99/max | digest interval p50/p99/max | tick overrun |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 39.5 | 0.406/0.642ms | 45.1/81.9/87.0µs | 102.273/106.060/106.067ms | 0 |
| 100 | 399.5 | 0.234/0.420ms | 28.7/45.1/53.8µs | 101.288/102.774/102.814ms | 0 |
| 1,000 | 3,929.3 | 0.189/0.294ms | 12.3/30.7/50.4µs | 100.161/101.067/101.077ms | 0 |
| 10,000 | 3,930.2 | 0.167/0.270ms | 14.3/36.9/51.0µs | 100.132/105.305/105.305ms | 0 |

1,000과 10,000 RPS/client 목표의 실효 처리량은 약 3,930 response/s에서 같게 멈춰 load
generator 상한을 보였다. 해당 구간에서 outbound high-water는 8, server admission 거절과 tick
overrun은 0이고 tick turn도 안정적이었다. 하나의 outstanding request만 만드는 load client의
timer/epoll 생성 한계가 Room보다 먼저 나타난 것이다. 따라서 이 실행이 보장하는 단일 Room
하한은 약 3,930 response/s이고, Room 자체의 붕괴점은 이 generator로 관측되지 않았다.

## 축 B — worker와 Room 수

정상 종료까지 성공한 가장 큰 점과 다음 실패점을 중심으로 sweep했다. outbound는 모든
worker가 공유하는 queue high-water다.

| workers × Rooms | clients | 결과 | response/s | gameplay RTT p99 | digest p99 | tick turn p99/max | outbound HWM |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 × 64 | 256 | 성공 | 2,528 | 7.140ms | 107.475ms | 73.7/1,362.1µs | 320 |
| 1 × 256 | 1,024 | 성공 | 10,112 | 26.563ms | 110.981ms | 57.3/1,380.3µs | 1,503 |
| 1 × 512 | 2,048 | active 성공, 동시 close 실패 | 19,882.7 | 33.540ms | 113.092ms | 41.0/3,788.8µs | 2,441 |
| 2 × 500 | 2,000 | 성공 | 19,750 | 29.429ms | 112.825ms | 65.5/1,368.4µs | 2,422 |
| 4 × 700 | 2,800 | 성공 | 27,650 | 30.720ms | 111.375ms | 73.7/2,024.0µs | 3,232 |
| 4 × 750 | 3,000 | outbound 포화, 116 close | 28,989.7 | 33.065ms | 116.332ms | 98.3/1,995.3µs | 4,096 |
| 4 × 800 | 3,200 | outbound 포화, 361 close | 28,411.2 | 44.377ms | 111.126ms | 81.9/3,761.8µs | 4,096 |
| 4 × 1,000 | 4,000 | outbound 포화, 1,475 close | 26,962.5 | 39.761ms | 112.557ms | 61.4/1,454.6µs | 4,096 |

모든 점에서 `tick_overruns`, tick schedule rejection, worker `rejected_full`은 0이었다. 100ms tick
주기 초과도 시작되지 않았다. 먼저 드러난 경계는 두 가지다.

- worker 1·2에서 Room당 4개 연결이 동시에 닫힐 때 player close의 worker별 async reservation
  1,024개가 먼저 찼다. 1 worker × 512 Room과 2 workers × 512 Room은 active 부하를 처리했지만
  종료 drain에서 `Actor async operation was rejected before it started`로 끝났다. 2 workers × 500
  Room은 worker별 in-flight high-water 1,006/994로 정상 종료해 이 경계를 바로 아래에서 확인했다
- 전체 대상 Slash fanout을 포함한 현재 코드에서 4 workers × 750 Room은 shared outbound
  queue high-water가 capacity 4,096에 닿고 116 `outbound_admission_failures`가 발생했다. 바로 아래
  700 Room은 high-water 3,232에서 모든 2,800 연결이 정상 진행·종료했다. worker를 늘려도
  이 공유 queue는 비례 확장되지 않는다

따라서 이 구성에서 별도 TimerService나 Room hash co-location은 다음 작업이 아니다. timer는
관측 범위에서 100ms 주기를 지켰고, 더 큰 Room 수를 지원하려면 player close reservation과 shared
outbound capacity/credit을 먼저 용량 계약으로 다뤄야 한다.

## 보상·fanout·느린 client

모든 성공·포화 실행에서 아래 Step 4 카운터는 전부 0이었다. 다만 6초 sweep은 default boss 생성
시각 40초보다 먼저 끝나 clear와 grant를 만들지 않았다. 따라서 이 값은 포화 중 보상 경로의 성공을
뜻하지 않고, 해당 실행에서 보상 인계 경로가 호출되지 않은 구조적 0이다.

| counter | 관측값 |
| --- | ---: |
| `grant_tell_rejections` | 0 |
| `reward_snapshot_admission_rejections` | 0 |
| `reward_snapshot_retry_giveups` | 0 |
| `grant_load_failures` | 0 |

fanout은 성공점인 4 workers × 700 Room 실행의 server 종료 summary에서 161,567 frames,
25,405,302 bytes였고 oversized digest는 0이었다. 750 Room 이상의 연결 종료는 digest 손실
복구 문제가 아니라 shared outbound admission 포화의 연결 격리 정책이 작동한 결과다.

느린 client 통합 시나리오는 1 Room × 4명에서 한 socket만 읽기를 중단했다. 포화를 결정적으로
유도하기 위해 테스트의 `max_pending_send_bytes`를 `GameServerConfig` 기본값 1MiB에서 32KiB로
낮추고 accepted socket send buffer는 8KiB, 느린 client receive buffer는 1KiB로 제한했다. 남은 세
client는 별도 reader로 계속 수신한다. 이 테스트 설정의 per-session send 한계에 닿은 연결 1개만
닫혔고, 남은 세 client는 digest sequence를 한 번도 건너뛰지 않은 채 `ParticipantLeft`를 관찰하고
boss clear까지 진행했다. 닫힌 연결 수 대 계속 진행한 Room 수는 1 대 1이다. 살아 있는 client의
event gap이 없으므로 주기적 resync snapshot은 만들지 않는다.

## 검증

- Docker Debug: core 5개 test target 통과, 같은 build의 MySQL integration을 local 전용 DB로
  별도 실행해 통과
- Docker ASan·UBSan: core 5개와 MySQL integration 통과
- Docker TSan: core 5개와 MySQL integration 통과
- Release 부하 종료 smoke: 성공점은 active connection이 모두 닫히고 server summary까지 출력
