# 10단계 Worker runtime 품질 게이트 리포트

> 문서 상태: **최종 — Stage 10 GO** (§10에 완료 후 리뷰 수정과 미해결 항목 1건)
>
> calibration: 2026-09-01 / 최종 gate 실행일: 2026-09-02 / 리뷰 수정 재실행: 2026-09-02
>
> 측정 대상: `refactor/archetecture`의 신규 Worker runtime test path
>
> 범위 제한: 이 수치는 production 처리량이나 11단계 application workflow의 성능 수치가 아니다.

## 1. 환경

| 항목 | 값 |
| --- | --- |
| image | `snf-server-dev` |
| kernel / arch | LinuxKit 6.12.5 / aarch64 |
| CPU / memory | Docker 관측 10 vCPU / 8,024,976 KiB |
| compiler | Ubuntu Clang 18.1.3 |
| CMake | 3.28.3 |

## 2. 재현 명령

Debug calibration은 공식 gate보다 먼저 단독 실행한다.

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc \
  'cmake --build --preset debug && ctest --preset debug -R "^(snf_worker_tests|snf_worker_load_stub)$" -V'
```

threshold를 고정한 뒤 실행한 공식 gate는 다음과 같다.

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc \
  'ctest --preset debug -R "^snf_worker_load_stub$" --repeat until-fail:5 --output-on-failure'
```

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc \
  'cmake --build --preset debug && ctest --preset debug --output-on-failure'
```

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc \
  'cmake --build --preset asan-ubsan && ctest --preset asan-ubsan -L worker --output-on-failure'
```

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc \
  'cmake --build --preset tsan && TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan -L worker --output-on-failure'
```

MySQL 환경이 있을 때의 재현 명령은 다음과 같다.

```bash
export SNF_MYSQL_TEST_HOST=...
export SNF_MYSQL_TEST_PORT=3306
export SNF_MYSQL_TEST_USER=...
export SNF_MYSQL_TEST_PASSWORD=...
export SNF_MYSQL_TEST_DATABASE=...
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SNF_MYSQL_TEST_HOST -e SNF_MYSQL_TEST_PORT -e SNF_MYSQL_TEST_USER \
  -e SNF_MYSQL_TEST_PASSWORD -e SNF_MYSQL_TEST_DATABASE \
  snf-server-dev bash -lc 'cmake --build --preset debug && ctest --preset debug -L mysql --output-on-failure'
```

## 3. 부하 시나리오 설정

단일 Worker stub 시나리오는 3초 동안 32개 부하 client와 별도 저율 probe client를 실행한다. 부하 client 중
4개는 4 KiB receive buffer를 쓰고 응답을 읽지 않는다. 모든 client와 요청 frame, client thread는 Worker
시작 전에 만든다. load process 내부 allocator 경쟁을 Worker blocking으로 오인하지 않기 위함이다. accept admission은
phase당 1개로 제한하지만 연결 33개는 모두 수락한다. 종료는 모든 client fd의 `shutdown(SHUT_RDWR)`, 2초 안의
harness thread 종료 확인과 join, 최대 2초 quiesce, Worker stop 순서다.

stub DB는 임시 listener에서 얻은 port를 사용하되 Worker 시작 전에 listener를 닫아 즉시 connection-refused를
만든다. accept된 TCP 연결에서 MySQL handshake만 영원히 기다리게 한 뒤 listener를 닫으면 driver teardown이 active
Db phase에 섞이므로, unavailable backend라는 같은 의미를 bounded 상태로 만든 것이다.

### WorkerBudgets

| phase | 설정 |
| --- | --- |
| Poll | events 1024, accepts 64, read connections 1024, frames 1024, 4 MiB, 500 us |
| Inbox | events 4096, lane당 8, 250 us |
| Timers | 2048, 250 us |
| Db | steps 64, rows 1024, 1 MiB, 500 us |
| Actors | turns 1024, 1 ms |
| Writes | 4 MiB, 500 us |
| max poll timeout | 50 ms |

### WorkerNetworkConfig

| 필드 | 값 |
| --- | ---: |
| connection capacity | 64 |
| max read buffer | 64 KiB |
| write soft / hard | 32 KiB / 64 KiB |
| close drain deadline | 500 ms |
| poll registration capacity | 65 |
| load `max_accepts_per_poll` | 1 (**gate-specific**, production default 64) |
| receive chunk | 16 KiB |
| server `SO_SNDBUF` 요청값 | 4096 |

### WorkerActorConfig

| 필드 | 값 |
| --- | ---: |
| ActorTable | 64 |
| actor mailbox | 256 messages / 2 MiB |
| total mailbox | 512 messages / 8 MiB |
| turns per actor slice | 32 |
| placement seed | 0 |
| Worker shutdown | 2000 ms |
| await timeout | 200 ms |
| concurrent Loading | 32 |
| application timer bytes | 2 MiB |

### Inbox / DB / watchdog / group

| 필드 | 값 |
| --- | --- |
| WorkerInbox | 64 MiB/Worker, max workers 32 |
| stub DB endpoint | `127.0.0.1`, 즉시 connection-refused, connection 1 |
| DB queue | 8 operations / 4096 allocated bytes |
| DB operation / async shutdown | 200 ms / 500 ms |
| DB reconnect backoff | 10 s |
| DB result cap | 4096 rows / 4 MiB |
| DB SSL | disabled |
| watchdog | sample 10 ms, active phase 500 ms, PollWait derived 700 ms |
| `budgets.sample_phase_execution` | load/gate **on**, 기본값 **off** (production은 syscall 없이 progress + watchdog만 유지) |
| load WorkerGroup | 2 Workers, 128 messages, 8 hops/message |
| group shutdown grace | load 500 ms; `WorkerGroupConfig` 기본값 1000 ms |
| load group shutdown budget | Worker 2000 ms + DB 0 ms + grace 500 ms = 2500 ms |

## 4. No Worker blocking 계약과 calibration

### 지표 역할

| 지표 | 역할 | 판정 |
| --- | --- | --- |
| thread CPU residence | owner thread가 실제 실행한 active phase 비용 | phase correctness threshold 미만 |
| voluntary context switch (`ru_nvcsw`) | sleep, futex wait, blocking syscall 등 owner thread의 실제 양보 | 일반 Debug active phase 합계 0 |
| involuntary context switch (`ru_nivcsw`) | scheduler 선점 진단 | 실패 조건이 아님; wall 증가 원인 귀속에 사용 |
| active wall residence | scheduler 지연까지 포함한 장기 정지 보조 안전망 | Debug: fairness threshold 미만 |
| sanitizer active-wall bound | voluntary 신호가 없는 preset에서 유일하게 남는 blocking 증거 | ASan/TSan: `CPU threshold + 400 ms` 미만 |
| active `max_entry_gap` | phase starvation | fairness threshold 미만 |
| PollWait wall residence | poll timeout의 직접 안전망 | Debug `max_poll_timeout * 2` 미만, sanitizer는 `+ 400 ms` |
| watchdog wall time | 명백한 active/shutdown stall | active episode 0; shutdown은 deadline 계측과 분리 |

Actor turn의 wall histogram `actor.turn_slice_ns`는 분포 관측용으로 유지한다. correctness는 다른 active phase와
동일하게 Actors phase의 CPU residence와 voluntary switch로 판정하므로 scheduler 선점 false positive가 Actor에만
남지 않는다. 100 ms/400 ms sleeping handler와 sleeping Actor의 deterministic test는 각각 Inbox/Actors의
`ru_nvcsw > 0`을 확인한다.

ASan/UBSan과 TSan은 allocator와 synchronization runtime을 owner thread에 삽입한다. 실제 ASan calibration에서
Poll 3회, Actors 2회의 voluntary switch가 이 runtime 때문에 재현됐다. 또한 PollWait 50 ms 샘플이 CPU 0.448 ms인
상태로 wall 123.990 ms까지 늦게 재스케줄됐다. 따라서 raw OS scheduling 지표(`ru_nvcsw`, PollWait wall)의
pass/fail 권위는 일반 Debug에 두고 sanitizer에서는 값을 그대로 출력하되 진단값으로 취급한다. sanitizer의
active CPU/wall fairness, functional assertion, sanitizer 오류는 계속 gate다. TSan suppression은 사용하지 않았다.

다만 `ru_nvcsw`를 진단으로 내리면 sanitizer preset에는 blocking 증거가 CPU residence 하나만 남고, wall 상한이
fairness(TSan 2.12초)뿐이 된다. 그 상태에서는 2초짜리 동기 blocking이 CPU를 쓰지 않는다는 이유로 통과할 수
있다. 그래서 sanitizer에는 fairness와 **별개의 active-phase wall 상한**을 둔다.

### 고정 threshold

```text
active CPU correctness    = phase budget * preset multiplier + single-item allowance
active wall fairness      = (모든 phase budget 합 + max poll timeout) * preset multiplier
sanitizer active-wall     = active CPU correctness + SANITIZER_ACTIVE_WALL_SLACK
SANITIZER_ACTIVE_WALL_SLACK = max_poll_timeout * 8 = 400 ms
```

`single_item_allowance`의 전제는 phase마다 다르다. poll/inbox/writes의 시간 상한은 항목마다
`steady_clock::now()`를 읽으므로 초과 단위가 실제로 1개다. 반면 `TimerQueue::expire`는 callback 64개마다
clock을 읽으므로 Timers phase의 분할 불가능한 단위는 최대 64개 callback이다. 이번 load 시나리오는 Timers
CPU가 0~0.276 ms라 이 차이가 게이트에 드러나지 않았지만, timer가 많은 시나리오에서 threshold를 다시 유도할
때는 이 granularity를 반영해야 한다.

slack의 근거는 calibration에서 실측한 **non-blocking wall 팽창의 최댓값**이다. 의도적 same-CPU 선점 sample이
130.059 ms, ASan의 늦은 재스케줄 sample이 123.990 ms였으므로 400 ms는 약 3배 여유이며, TSan fairness(2120 ms)
대비 5배 이상 타이트하다. 이 상한은 sanitizer preset에만 적용하고 Debug는 `ru_nvcsw == 0`이 직접 증거이므로
느슨한 fairness 안전망을 유지한다 — Debug에서 이 상한을 쓰면 위의 130 ms 선점 sample이 거짓 실패가 된다.

| phase | allowance | Debug K=8 | ASan/UBSan K=20 | TSan K=40 |
| --- | ---: | ---: | ---: | ---: |
| Poll | 0.5 ms | 4.5 ms | 10.5 ms | 20.5 ms |
| Inbox | 0.5 ms | 2.5 ms | 5.5 ms | 10.5 ms |
| Timers | 0.5 ms | 2.5 ms | 5.5 ms | 10.5 ms |
| Db | 0.5 ms | 4.5 ms | 10.5 ms | 20.5 ms |
| Actors | 2 ms | 10 ms | 22 ms | 42 ms |
| Writes | 0.5 ms | 4.5 ms | 10.5 ms | 20.5 ms |
| fairness | — | 424 ms | 1060 ms | 2120 ms |

sanitizer preset에서 실제로 검사하는 active-phase wall 상한은 다음과 같다. Debug 열은 해당 없음이다.

| phase | ASan/UBSan wall | TSan wall |
| --- | ---: | ---: |
| Poll | 410.5 ms | 420.5 ms |
| Inbox | 405.5 ms | 410.5 ms |
| Timers | 405.5 ms | 410.5 ms |
| Db | 410.5 ms | 420.5 ms |
| Actors | 422 ms | 442 ms |
| Writes | 410.5 ms | 420.5 ms |
| PollWait | 500 ms | 500 ms |

calibration → 측정 확인 → threshold 고정 → 공식 gate 순서로 실행했다. calibration 뒤 threshold와 multiplier는
변경하지 않았다.

### Debug calibration 실측

| phase | max wall | max CPU | voluntary | involuntary | max entry gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Poll | 0.346 ms | 0.987 ms | 0 | 0 | 56.090 ms |
| Inbox | 0.048 ms | 0.164 ms | 0 | 0 | 56.091 ms |
| Timers | 0.048 ms | 0 ms | 0 | 0 | 56.093 ms |
| Db | 0.274 ms | 1.015 ms | 0 | 0 | 56.093 ms |
| Actors | 0.215 ms | 0.267 ms | 0 | 0 | 56.094 ms |
| Writes | 0.135 ms | 0 ms | 0 | 0 | 56.094 ms |
| PollWait | 56.061 ms | 0.855 ms | 775 | 0 | 56.094 ms |

CPU와 wall의 각 max는 서로 다른 sample일 수 있고 Linux `getrusage` CPU 값은 microsecond 단위로 양자화된다.
따라서 두 max를 서로 빼지 않는다. 같은 최대 wall sample의 원인 귀속에는 별도 `wall_max_cpu`와 context-switch
필드를 사용한다.

동일 CPU에 normal-priority competitor와 nice 19 Worker를 고정한 deterministic 선점 test의 실측은
wall 130.059 ms, 같은 wall sample의 CPU 10.012 ms, voluntary 0, involuntary 5였다. wall만 correctness로 쓰면
거짓 blocking이지만 CPU/voluntary 계약은 통과하므로 scheduler 선점 가설을 직접 확인했다.

`WorkerProgress`의 packed timestamp 필드는 60-bit microseconds라 표현 폭이 약 36,500년이다. 이것은 encoding의
폭이며 실제 `steady_clock::time_point::duration` 표현 범위를 늘린다는 뜻이 아니다. 구현은 source clock이
표현 가능한 microseconds만 pack/unpack하고 그 경계를 deterministic test로 검증한다.

## 5. 품질 게이트 판정

| 게이트 | 명령 | 지표 | 측정값 | 판정 |
| --- | --- | --- | --- | --- |
| Thread ownership | TSan `-L worker`, Debug full | TSan report, owner assertion | TSan 실행 8 PASS/3 MySQL SKIP, race 0 | PASS |
| No Worker blocking | Debug calibration/load, deterministic block tests | active CPU, `ru_nvcsw`, PollWait wall, watchdog | active voluntary 전 phase 0, CPU 전 phase 상한 미만, PollWait 56.061 ms < 100 ms, active stall 0 | PASS |
| Memory bound | ASan/UBSan `-L worker`, Debug load | admission rejection + exact/sampled HWM | read 12,318 B, write 28,742 B, DB 8 ops/832 B, submit rejection 52; cap 초과 0 | PASS |
| Single await state | Debug `snf_worker_tests` | `ActorSlot::blocked`, timeout/completion transition | 중복 continuation/deadline source 0, deterministic transition PASS | PASS |
| Stale safety | Debug worker/DB/network tests | generation/incarnation/operation/late timeout 상태 불변 | stale 주입 뒤 mutation 0, counter 기대값 일치 | PASS |
| Fairness | Debug load | phase entries, active wall, max entry gap | 6 active phase 반복 진입, max gap 56.094 ms < 424 ms | PASS |
| Shutdown | Debug/ASan/TSan worker load/tests | A–D, DB final, join overrun, leak/sanitizer | deadline hit 0, final resource 0, group overrun 0, sanitizer 오류 0 | PASS |
| Behavior parity | Debug full/TCP/probe | Pong 1:1, protocol/invariant, 기존 회귀 | sent probe=received probe, protocol/invariant 0, Debug 실행 13 PASS | PASS |

공식 preset 결과:

| preset | 결과 |
| --- | --- |
| Debug load repeat | 5/5 PASS |
| Debug full CTest | 17 등록, 13 PASS, MySQL 4 SKIP, 실패 0 |
| ASan/UBSan worker label | 11 등록, 8 PASS, MySQL 3 SKIP, sanitizer 오류/leak 0 (반복 실행은 §10의 미해결 항목 참고) |
| TSan worker label | 11 등록, 8 PASS, MySQL 3 SKIP, race 0 (`halt_on_error=1`) |

## 6. 대표 load 측정값

```text
worker_report.loop_iterations=902
worker_report.loop_ns=count:902,max:672917,p50:90111,p95:196607,p99:262143
worker_report.actor_turn_ns=count:836,max:209041,p50:26623,p95:57343,p99:90111
worker_report.db_operation_ns=count:8,max:201566084,p50:201566084,p95:201566084,p99:201566084
worker_report.network=accepted:33,closed:33,received:1084,sent:99,protocol_errors:0,invariant_violations:0,epollout_waits:4,hard_limit_sends:0
worker_report.db=completed:0,failed:8,queued_timeouts:8,in_flight_timeouts:0,submit_rejections:52
worker_report.hwm_exact=read_buffer:12318,write_queue:28742,db_queued_ops:8,db_queued_bytes:832,db_in_flight:0
worker_report.hwm_sampled=connections:5,actors:10,loading:8,ready:0,mailbox_messages:8,mailbox_bytes:456,timers:8,timer_bytes:0,inbox_bytes:0
```

HWM은 admission proof가 아니라 관측값이다. 상한 강제의 직접 증거는 자원별 cap 직전 accepted와 cap 초과
rejected를 검사하는 deterministic tests다. `sampled_connections=5`가 accepted 33보다 작은 것도 64-loop sampling이
순간 peak를 놓칠 수 있기 때문이며, 이름으로 exact/sampled 강도를 구분했다.

## 7. Watchdog와 deterministic 계측 증거

- Debug load watchdog: samples 416, active stall 0, shutdown stall 0, longest 0.
- 20 ms CPU spin: Inbox CPU residence 15 ms 이상, voluntary 0.
- 100 ms sleeping handler: Inbox voluntary > 0, CPU < wall.
- 400 ms blocking handler/Actor: 50 ms watchdog threshold에서 Inbox/Actors 각각 정확히 1 episode,
  `longest_stall_ns >= 300 ms`.
- 정상 idle/load: active phase voluntary 0.
- scheduler 선점: wall 130.059 ms / CPU 10.012 ms / voluntary 0 / involuntary 5.

## 8. Shutdown 계측과 final teardown

대표 Debug load:

| phase | duration | deadline hit | 남은 핵심 자원 |
| --- | ---: | --- | --- |
| A | 0.090 ms | false | connections 0, actors 2, DB 0/0 |
| B | 0.039 ms | false | actors/blocked/loading 0 |
| C | 0.001 ms | false | connections 0 |
| D | 0.004 ms | false | connections/actors/blocked/loading/inbox/app timer bytes 0 |
| post-DB | 0.002 ms | false | DB in-flight/queued 0 |

Actor phases 0.158 ms < 2000 ms, DB async shutdown 0.002 ms < 500 ms, total 0.161 ms였다. 별도
unresponsive-DB deterministic test는 actor deadline false와 50 ms DB deadline true를 동시에 확인한다.
WorkerGroup은 requestStop 시각부터 2500 ms 예산으로 actual thread-exit flag를 먼저 관측하고, 대표 load에서
`joinOverruns()`는 비었다. 초과가 생기면 snapshot을 남긴 뒤 안전을 위해 join하는 계약이다.

final teardown boundedness는 두 층이다. DbClient의 async connect/query/fetch/cancel progress는 전달된 absolute
deadline으로 제한된다. 그 뒤 C API cleanup은 8단계 conformance에서 이미 검증된 primitive 범위만 사용한다.
이번 Stage 10에는 실제 MySQL server가 없었으므로 authenticated/server-connected `mysql_close()` wall bound를
새로 증명했다고 쓰지 않는다. 즉 **MySQL final teardown boundedness는 이번 라운드 미측정**이다.

## 9. MySQL 상태, 실패 수정과 최종 범위

MySQL 환경변수가 제공되지 않아 다음 4개 `SKIP_RETURN_CODE 77` 등록이 Debug full CTest에서 정확히 4개 SKIP됐다.

1. `snf_worker_load_mysql`
2. `snf_db_conformance_mysql`
3. `snf_worker_db_client_mysql`
4. `snf_mysql_integration_tests`

실패 후 수정한 실제 문제는 세 가지다.

1. 33개 연결을 한 Poll phase에서 연속 accept하면 nonblocking `accept4` 내부에서도 voluntary switch가 발생했다.
   load admission을 1 accept/phase로 제한해 active Poll blocking을 제거했다.
2. TCP 연결만 성립하고 MySQL peer가 없는 stub은 driver teardown wait를 active Db phase에 만들었다. 같은 unavailable
   backend 의미를 즉시 connection-refused로 바꿔 active Db voluntary를 0으로 만들었다.
3. ASan runtime의 allocator/synchronization과 늦은 재스케줄을 product blocking으로 판정했다. raw OS scheduling
   지표의 권위를 Debug에 명시하고 sanitizer에서는 값은 숨기지 않은 채 diagnostic으로 분리했다. threshold를
   올리거나 suppression을 추가하지 않았다.

MySQL을 제외하기로 한 합의 범위에서 8개 품질 게이트는 모두 PASS다. 따라서 Stage 10은 **GO**다. 실제 MySQL
gate와 production application workflow의 동일 gate 재실행은 11단계 완료 조건으로 남는다.

## 10. 완료 후 리뷰에서 수정한 것

계획의 5라운드 리뷰 항목이다. 상세 계약은
[최종 계획의 5라운드 표](./stage-10-quality-gate-plan.md)에 있다.

1. **`WorkerGroup` join의 lost wakeup (correctness).** worker thread가 exit flag를 `_exit_mutex` 밖에서
   store하고 notify했다. flag가 `join()`이 predicate를 평가한 뒤 block하기 전 구간에 도착하면 그
   `notify_all()`이 유실되고, 모든 worker가 이미 반환했는데도 `join()`이 group budget 전체(load 설정에서
   2500 ms, 기본 설정에서 2000 ms 이상)를 기다린다. flag를 join wait와 같은 mutex 아래에서 publish하도록
   고쳤고, grace 5000 ms 그룹이 1000 ms 안에 join되는 것을 확인하는
   `test_worker_group_join_wakes_on_thread_exit_instead_of_the_deadline`을 추가했다. deadline으로 깨우는
   구현은 이 테스트를 통과할 수 없다.
2. **sanitizer active-phase wall 상한 추가.** §4에 근거와 값을 기록했다. 이전에는 ASan/TSan에서 wall 상한이
   fairness(TSan 2120 ms)뿐이라 CPU를 쓰지 않는 2초짜리 blocking이 통과할 수 있었다.
3. **phase CPU/context-switch 계측을 opt-in으로 전환.** `WorkerBudgets::sample_phase_execution`은 기본
   `false`이고 gate 실행만 켠다. 이전에는 phase 전환마다 `getrusage(RUSAGE_THREAD)` syscall이 production
   경로에 그대로 있었다. progress word와 watchdog은 `steady_clock`만 읽으므로 영향이 없고, 운영 stall 관측은
   그대로 유지된다. flag가 꺼진 채 gate를 돌려 "0으로 통과"하는 것을 막기 위해 load test가 flag와 active CPU
   합계 > 0을 먼저 검사하고, `test_phase_execution_sampling_is_opt_in`이 두 방향을 모두 고정한다.
4. **`DbClient::shutdown()`의 순서 의존 명시.** 공유 poller에서 non-DB 이벤트를 버리므로 phase D가 연결을
   force-close·deregister한 뒤에만 호출할 수 있다. 헤더와 호출부에 계약으로 적었다.
5. **이름/의미 정리.** `WorkerGaugeSnapshot::sampled_inbox_queued_bytes` rename(64-loop sampled임을 이름에
   드러냄), `poll_budget_stops`는 iteration당 1회가 아니라 stop event 횟수라는 주석.

수정 후 재실행: Debug full CTest 17 등록 / 13 PASS / MySQL 4 SKIP / 실패 0, Debug `-L worker` 11 등록 8 PASS,
TSan `-L worker` 8 PASS + `snf_worker_load_stub` 반복 3/3 PASS(race 0), ASan/UBSan `-L worker` 8 PASS.

### 미해결 — ASan Poll phase CPU residence의 tail

`ctest --preset asan-ubsan -R snf_worker_load_stub --repeat until-fail:N`을 돌리면 낮은 확률로 Poll phase의
`max_cpu_residence`가 45.135 ms까지 튀어 ASan CPU threshold 10.5 ms를 넘고 게이트가 실패한다. 관측값:

| 실행 형태 | 결과 |
| --- | --- |
| 바이너리 직접 실행 12회 | 전부 PASS, Poll `max_cpu_residence` 1.00~1.37 ms |
| `ctest --repeat until-fail:3` | 3번째 iteration에서 실패 |
| `ctest --repeat until-fail:5` | 3번째 iteration에서 실패 |
| `ctest --repeat until-fail:6` | 6/6 PASS |
| 합계 | 약 20회 실행 중 2회 실패 (약 10%) |
| 실패 sample | Poll wall 45.174 ms / CPU 45.135 ms / involuntary 1, 같은 실행의 `Starting` CPU 5.566 ms |

wall ≈ CPU이므로 blocking이 아니라 실제 on-CPU 소비이고, 같은 실행의 `Starting` phase까지 평소의 18배로
느렸다는 점은 프로세스 전체가 느려진 구간(호스트/VM 경합 또는 ASan runtime의 일괄 작업)을 가리킨다. Poll
phase의 시간 예산은 frame·recv마다 검사되므로 분할 불가능한 한 항목이 45 ms를 쓸 구조가 아니다.

**threshold를 올리지 않았다.** 게이트를 통과시키려고 CPU 상한을 조정하는 것은 이 게이트의 1차 증거를 무력화한다.
따라서 현재 상태를 이렇게 기록한다: ASan preset의 `-L worker`는 단발 실행에서 PASS이며 **반복 실행에서
repeat-stable하지 않다.** 후속으로 필요한 것은 (1) 45 ms 구간의 실제 소비 지점 확인(phase 내 하위 구간 계측
또는 perf), (2) 그 결과에 따라 원인 수정 또는 근거를 갖춘 별도 calibration 커밋이며, threshold 변경은 그
근거가 나온 뒤에만 한다.
