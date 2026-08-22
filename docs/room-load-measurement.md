# Step 5 Room 부하 측정 리포트

측정일은 2026-08-22이다. `snf-server-dev:latest`의 Release build를 Docker Linux
6.12.5-linuxkit/aarch64에서 실행했다. container에 보인 자원은 10 CPU, 약 7.65 GiB memory이고
compiler는 GCC 13.3.0이다. 각 값은 6초 단일 실행의 용량 탐색 결과이므로 장기 benchmark의 신뢰
구간이 아니라 설계 경계와 다음 병목을 찾는 자료다.

## 방법

공통 workload는 `LoadScenario::Battle`, Room당 4명이다. 축 B는 client당 10 RPS로 고정하고 축 A만
목표 RPS를 바꿨다. 모든 client가
`Authenticate` → `EnterZone` → `RoomJoin`을 마친 뒤 방별 leader 하나가 `BattleStart`를 보내고,
참가자는 `UseSkill`과 `SetMoveIntent`를 번갈아 보낸다. default 100ms Room tick과 전투 규칙을
사용했다. server는 5초 시계열과 종료 summary를, load client는 요청 RTT와 push frame 수·bytes,
digest 도착 간격을 기록했다.

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

## 축 A — 단일 Room 직렬화

1 Room × 4 client에서 목표 RPS만 올렸다. RTT는 gameplay response, tick 값은 server의
`tick_turn_nanoseconds`다.

| 목표 RPS/client | 실효 response/s | gameplay RTT p50/p99 | tick turn p50/p99/max | digest interval p50/p99/max | tick overrun |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 39.5 | 0.326/0.778ms | 41.0/66.6/66.6µs | 102.248/106.271/106.273ms | 0 |
| 100 | 398.8 | 0.259/0.577ms | 28.7/71.2/71.2µs | 101.119/103.100/103.111ms | 0 |
| 1,000 | 3,982.8 | 0.187/0.242ms | 16.4/64.2/64.2µs | 100.137/101.013/101.019ms | 0 |
| 10,000 | 2,028.2 | 0.197/0.327ms | 16.4/61.8/61.8µs | 100.689/109.889/109.909ms | 0 |

1,000 RPS/client까지 Room queue와 tick은 안정적이었다. 10,000 목표에서 실효 처리량이 낮아졌지만
server worker queue high-water는 8, mailbox high-water는 5, `rejected_full`은 0이고 tick turn도
변하지 않았다. 하나의 outstanding request만 만드는 load client의 100µs timer/epoll 생성 한계가
Room보다 먼저 나타난 것이다. 따라서 이 실행이 보장하는 단일 Room 하한은 약 3,983 response/s이고,
Room 자체의 붕괴점은 이 generator로 관측되지 않았다.

## 축 B — worker와 Room 수

정상 종료까지 성공한 가장 큰 점과 다음 실패점을 중심으로 sweep했다. queue와 mailbox는 worker별
high-water 배열이고, outbound는 모든 worker가 공유하는 queue high-water다.

| workers × Rooms | clients | 결과 | response/s | gameplay RTT p99 | digest p99 | tick turn p99/max | timer lateness p99 | worker queue HWM | mailbox HWM | outbound HWM |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |
| 1 × 64 | 256 | 성공 | 2,528 | 6.878ms | 107.136ms | 65.5/649.3µs | 6.29ms | 260 | 112 | 448 |
| 1 × 256 | 1,024 | 성공 | 10,112 | 27.440ms | 116.627ms | 57.3/2,576.8µs | 5.77ms | 1,013 | 496 | 1,323 |
| 1 × 512 | 2,048 | active 성공, 동시 close 실패 | 20,224 | 31.897ms | 113.831ms | 49.2/1,120.6µs | 5.77ms | 2,675 | 1,607 | 2,259 |
| 2 × 500 | 2,000 | 성공 | 19,750 | 34.747ms | 113.984ms | 57.3/2,036.8µs | 6.00ms | 1,018 / 994 | 185 / 173 | 2,173 |
| 4 × 800 | 3,200 | 성공 | 31,600 | 31.192ms | 110.687ms | 81.9/2,153.7µs | 5.77ms | 807 / 814 / 791 / 800 | 160 / 115 / 121 / 158 | 3,806 |
| 4 × 1,000 | 4,000 | outbound 포화, 865 close | 32,196 | 37.309ms | 113.439ms | 65.5/2,056.5µs | 5.98ms | 775 / 664 / 745 / 863 | 147 / 115 / 195 / 173 | 4,096 |

모든 점에서 `tick_overruns`, tick schedule rejection, worker `rejected_full`은 0이었다. 100ms tick
주기 초과도 시작되지 않았다. 먼저 드러난 경계는 두 가지다.

- worker 1·2에서 Room당 4개 연결이 동시에 닫힐 때 player close의 worker별 async reservation
  1,024개가 먼저 찼다. 1 worker × 512 Room과 2 workers × 512 Room은 active 부하를 처리했지만
  종료 drain에서 `Actor async operation was rejected before it started`로 끝났다. 2 workers × 500
  Room은 worker별 in-flight high-water 1,006/994로 정상 종료해 이 경계를 바로 아래에서 확인했다
- 4 workers × 1,000 Room은 shared outbound queue high-water가 capacity 4,096에 닿고 865
  `outbound_admission_failures`가 발생했다. 같은 설정의 800 Room은 high-water 3,806에서 모든
  3,200 연결이 정상 진행·종료했다. worker를 늘려도 이 공유 queue는 비례 확장되지 않는다

따라서 이 구성에서 별도 TimerService나 Room hash co-location은 다음 작업이 아니다. timer는
관측 범위에서 100ms 주기를 지켰고, 더 큰 Room 수를 지원하려면 player close reservation과 shared
outbound capacity/credit을 먼저 용량 계약으로 다뤄야 한다.

## 보상·fanout·느린 client

모든 성공·포화 실행에서 아래 Step 4 카운터는 전부 0이었다.

| counter | 관측값 |
| --- | ---: |
| `grant_tell_rejections` | 0 |
| `reward_snapshot_admission_rejections` | 0 |
| `reward_snapshot_retry_giveups` | 0 |
| `grant_load_failures` | 0 |

fanout은 4 workers × 800 Room 실행에서 184,674 frames, 39,625,488 bytes였고 oversized digest는
0이었다. 4 workers × 1,000 Room의 연결 종료는 digest 손실 복구 문제가 아니라 shared outbound
admission 포화의 연결 격리 정책이 작동한 결과다.

느린 client 통합 시나리오는 1 Room × 4명에서 한 socket만 읽기를 중단했다. 2KiB per-session send
한계에 닿은 연결 1개만 닫혔고, 남은 세 client는 digest sequence를 한 번도 건너뛰지 않은 채
`ParticipantLeft`를 관찰하고 boss clear까지 진행했다. 닫힌 연결 수 대 계속 진행한 Room 수는
1 대 1이다. 살아 있는 client의 event gap이 없으므로 주기적 resync snapshot은 만들지 않는다.

## 검증

- Docker Debug 전체: 5개 test target 통과, MySQL integration은 환경 미설정으로 skip
- Docker ASan·UBSan 전체: 동일
- Docker TSan 전체: 동일
- Release 부하 종료 smoke: 성공점은 active connection이 모두 닫히고 server summary까지 출력
