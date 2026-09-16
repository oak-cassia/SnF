> 문서 상태: **최종**
>
> 확정일: 2026-09-01
> 적용 범위: Unified Worker Runtime 전환의 10단계 구현·검증
>
> 이 문서는 [개발 로드맵](./development-roadmap.md)의 10단계를 수행할 때 따르는 상세 실행 기준이다.
> 구현 중 전제가 달라지면 코드만 우회하지 말고 이 문서와 로드맵을 함께 갱신한다.

# 10단계 — metrics, watchdog, shutdown과 load test

## Context

`refactor/archetecture` 브랜치의 Unified Worker Runtime 전환에서 0~9단계는 모두 닫혔다
(9단계는 조건 불충족으로 생략). 남은 첫 열린 단계가 10단계이며, 요구사항은
[docs/development-roadmap.md:155](./development-roadmap.md#L155)에 정의되어 있다.

지금까지의 단계는 **동작**을 만들었지만 그 동작이 목표 아키텍처의 품질 기준을 지키는지
**관측할 수단이 없다**. 구체적으로:

- `WorkerMetrics`는 이미 존재하지만(약 75개 counter) **gauge, high-water mark, latency 분포가 하나도 없다.**
  유일한 latency는 `WorkerActorMetrics::max_slice_duration` 뿐이다.
- event loop 6개 phase 어디에도 **진행 시각(progress timestamp)이 없다.** 따라서 Worker stall을
  외부에서 감지할 방법이 없고, watchdog도 없다.
- `WorkerMetrics`는 non-atomic plain struct이고 계약상
  ([include/snf/worker/worker.hpp:273](../include/snf/worker/worker.hpp#L273)) owner thread 또는 join 이후에만
  읽을 수 있다. 즉 **실행 중 cross-thread 관측 경로가 아예 없다.**
- graceful shutdown은 phase A~D와 절대 deadline이 구현돼 있지만 계측이 5개 counter뿐이라
  "hard deadline 안에 끝났는가"를 증명할 수 없다.
- 신규 Worker runtime을 client I/O + hot Actor + 느린 DB + slow consumer로 **동시에** 밀어보는
  부하 시나리오가 없다.

목표: 8개 품질 게이트([docs/development-roadmap.md:193](./development-roadmap.md#L193))를 신규 Worker
runtime **test path**에서 관측 가능하게 만들고 모두 통과시킨 뒤, 재현 명령·설정·측정값·판정을 문서로 남긴다.
11단계(production 경로 전환)에서 같은 표를 다시 실행한다.

### 확정된 결정 (사용자 확인 완료)

1. `DbClientConfig::max_queued_bytes`는 **지금 강제한다.** 현재 검증만 되고
   [src/worker/db_client.cpp](../src/worker/db_client.cpp)의 `tryStart`는 `max_queued_operations`만 본다.
   Memory bound 게이트가 "DB queue"를 명시하므로 강제하지 않으면 게이트가 거짓이 된다.
2. MySQL 게이트는 **stub 경로만 실측 기록**하고 MySQL 경로는 재현 명령만 남긴 뒤 "이번 라운드 미측정"으로
   명시한다.
3. `WorkerGroup::join()`에 **관측만 추가한다.** 초과분을 기록하되 항상 join한다. `detach()`는 UAF라 하지 않는다.

### 리뷰 반영 (초안 대비 변경점)

초안의 게이트 중 일부가 **실제로 틀린 것을 잡아내지 못하는 assert**였다. 2라운드 반영 내역:

**1라운드**

| # | 변경 | 위치 |
| --- | --- | --- |
| 1 | group 완료 판정을 `progress == Stopped` → **`thread_exited` atomic**. deadline 시작점도 `join()` → **`requestStop()`** | 10F |
| 2 | shutdown 예산을 **Actor(A~D)와 DB로 분리** | 10E |
| 3 | Fairness를 `entries > 0` → **`max_entry_gap < threshold`** 추가 | D1 |
| 4 | Stale safety를 load test의 `합 > 0`(flaky) → **deterministic 주입 + 상태 unchanged assert** | 10G-stale |
| 5 | No Worker blocking을 `p99` → **`max`** | D1 |
| 6 | slow consumer watermark 하향, parity는 **분리된 저율 probe** | 10G |
| 7 | DB byte accounting에 **감소 경로 전체 계약 + `queuedFootprint()`** | 10A |

**2라운드** — 아래 7개는 각각 flaky·거짓통과·미보장을 만든다

| # | 문제 | 수정 | 위치 |
| --- | --- | --- | --- |
| 1 | `join()`을 먼저 부르면 첫 멈춘 thread에서 막혀 deadline 검사 자체가 불가능 | **observe-before-join** 알고리즘을 순서까지 고정 | 10F |
| 2 | `shutdown()` 반환 뒤 시간을 재는 것은 hard deadline **보장이 아니라 사후 인지** | deadline을 `shutdown(poller, deadline)`으로 **내부 루프까지 전달** | 10E |
| 3 | 전부 edge-triggered면 `longest_stall_ns`가 threshold 근처에 머물러 `>=300ms` 테스트와 충돌 | episode/callback만 edge, **`longest_stall_ns`는 stalled 매 sample마다 `max()`** | D2 |
| 4 | hot actor는 **같은 `ActorKey`라 activation load가 1건** — DB queue가 포화되지 않아 `queued_timeouts > 0`이 거짓 전제 | 증거를 `operation_timeouts > 0` + latency로 교체, **DB queue 포화는 다중 ActorKey 별도 케이스** | 10G, D3 |
| 5 | client `SO_RCVBUF`만으론 작은 Pong이 kernel send buffer에 계속 들어가 EAGAIN 미보장 (TSan에서 악화) | **server측 `SO_SNDBUF` 하향(test-only)** 으로 kernel backpressure까지 강제 | 10G |
| 6 | writer/probe가 blocking syscall에 걸리면 `atomic<bool>`로 안 멈춰 **Worker가 아니라 test client 때문에** timeout | client lifecycle을 **non-blocking + `shutdown(fd, SHUT_RDWR)` 후 join**으로 고정 | 10G |
| 7 | `fairness_threshold` 값·유도식 없음. watchdog 여유값(250ms)을 correctness 상한으로 쓰면 100ms 동기 block이 통과 | **진단 threshold와 correctness threshold를 분리**하고 둘 다 budget에서 유도. shutdown episode를 active-loop stall과 분리 | D4(신설), 10G |
| 부수 | `WorkerHighWaterMarks`만 있고 current gauge가 없는데 제목은 "gauge" | **`WorkerGaugeSnapshot` 추가** (요구사항이 gauge를 명시) | 10A |
| 부수 | `queued_bytes == 0 iff empty`는 약한 검증, admission overflow, logical/allocated byte 모호 | Debug에서 **전체 합 검증**, overflow-safe 비교, **allocated(`capacity()`) 기준** 명시 | 10A |
| 부수 | shutdown 판정에서 blocked/loading/timer/inbox 잔여가 빠짐 | 자원별로 **0 필수 / 잔여 허용**을 표에 명시 | 10G |
| 부수 | 재현 명령이 preset build를 포함하지 않음 | build+ctest **한 줄**, 정규식도 `-R 'a\|b'` 형태로 | 10H |

**3라운드** — 관측을 증명으로 착각한 지점을 닫는다

| # | 문제 | 수정 | 위치 |
| --- | --- | --- | --- |
| 1 | `phase_d`에 DB 잔여를 넣으면 그 값은 **DB shutdown 이전** 상태 | `WorkerResourceSnapshot`을 분리하고 **`final_resources`(post-DB) 신설**. 판정도 시점별로 나눔 | 10E, 10G |
| 2 | "deadline 안에 무조건 반환"은 C API cleanup까지 선점 가능하다는 전제가 없으면 **지킬 수 없는 약속** | 계약을 **async progress(deadline-bounded) / final teardown(conformance-backed)** 두 층으로. `mysql_close()` boundedness는 10H에 근거 또는 미측정으로 명시 | 10E, 10H |
| 3 | threshold를 안 정하고 실행하면 **test-to-pass tuning**이 됨 | 공식·phase별 `single_item_allowance`·preset multiplier를 **실행 전에 고정**. calibration과 gate 실행 분리. DB timeout도 단일 하한 → **band** | D4, 10G |
| 4 | 종합 load에서 EAGAIN이 "운 좋게" 나기를 기다리는 것은 게이트로서 약함 | **deterministic slow-consumer 테스트 분리** (bounded attempts 안에 EAGAIN 못 만들면 FAIL). 종합 load는 압력만 담당 | 10G-slow |
| 5 | 64반복 샘플은 엄밀히 HWM이 아니고, HWM은 "cap을 못 넘는다"의 증명이 아님 | **exact/sampled를 이름으로 구분**. Memory bound 1차 근거를 **admission/rejection 테스트**로 이동, HWM은 관측 | 10A, 10G |

**4라운드 — calibration 후 최종 계측 계약 보정**

| # | 문제 | 최종 계약 | 위치 |
| --- | --- | --- | --- |
| 1 | active phase의 wall residence는 OS scheduler 선점까지 포함해 blocking false positive를 만든다 | correctness는 **thread CPU residence + voluntary context switch 0**, wall은 fairness/장기 정지 안전망, involuntary switch는 선점 귀속에 사용 | D1, D4, 10G |
| 2 | Actor turn의 wall histogram만 gate하면 같은 false positive가 Actor에 남는다 | Actor도 **Actors phase CPU/context-switch**로 판정하고 turn wall histogram은 분포 관측으로 유지 | D1, D4, 10G |
| 3 | 계측 자체가 CPU와 blocking을 구분한다는 증거가 없다 | spin/sleep/idle/same-CPU scheduler 선점 deterministic test를 추가하고 blocking handler의 voluntary switch를 검출 | D1, 10G |
| 4 | calibration 도중 threshold를 올리면 공식 gate가 자기참조가 된다 | calibration → 측정 검토 → threshold 고정 → 별도 공식 gate 순서를 기록하고 공식 gate 중 변경 금지 | D4, 10H |
| 5 | load harness의 accept burst와 handshake 없는 MySQL TCP peer가 active phase voluntary switch를 만들었다 | accept는 phase당 1건, client/thread는 Worker 전에 준비, stub은 Worker 시작 전에 listener를 닫아 즉시 connection-refused | 10G |
| 6 | sanitizer가 삽입한 allocator/synchronization과 늦은 재스케줄을 product blocking으로 오인했다 | raw OS scheduling gate의 권위는 Debug에 두고 sanitizer에서는 출력은 유지하되 진단값으로 분리. CPU/wall fairness, 기능 assert와 sanitizer 오류는 계속 gate | D4, 10G |
| 7 | MySQL skip 등록 수를 3개로 기록했다 | CMake의 실제 `SKIP_RETURN_CODE 77` **4개**를 이름까지 기록하고 이번 라운드는 MySQL 미측정으로 종료 | 10H |

**5라운드 — 완료 후 리뷰 반영**

| # | 문제 | 최종 계약 | 위치 |
| --- | --- | --- | --- |
| 1 | `WorkerGroup`의 exit flag를 `_exit_mutex` 밖에서 store하고 notify해 lost wakeup이 가능했다. 정상 종료인데도 `join()`이 group budget 전체를 기다릴 수 있다 | exit flag를 **join wait와 같은 mutex 아래에서 publish**하고, `join()`이 deadline이 아니라 flag로 깨는 것을 확인하는 deterministic test를 추가 | 10F |
| 2 | `ru_nvcsw`를 sanitizer에서 진단으로 내린 결과, ASan/TSan에는 wall 상한이 fairness(TSan 2.12초)뿐이라 2초짜리 동기 blocking이 통과할 수 있었다 | sanitizer preset에만 **fairness와 별개의 active-phase wall 상한**을 추가. 6라운드에서 이 상한을 `CPU threshold + witness slack`으로 조였고, 고정 400 ms는 설계상 blocking wait인 PollWait 전용(`SANITIZER_POLL_WAIT_SLACK`)으로 분리했다. Debug는 `ru_nvcsw == 0`이 직접 증거이므로 느슨한 fairness 안전망 유지 | D4, 10G |
| 3 | phase 전환마다 `getrusage(RUSAGE_THREAD)` syscall이 production hot path에 상주했다. 이 값은 게이트 증명용이다 | `WorkerBudgets::sample_phase_execution`으로 **opt-in(기본 off)**. gate 실행만 켠다. packed progress word와 watchdog은 `steady_clock`만 읽으므로 항상 켜 둔다. load gate는 flag가 켜졌는지와 active CPU 합계 > 0을 먼저 검사해 "0으로 통과"를 막는다 | D1, D4, 10G |
| 4 | `DbClient::shutdown()`이 공유 poller에서 non-DB 이벤트를 버리는 순서 의존이 숨은 전제였다 | phase D가 연결을 force-close·deregister한 **뒤에만** 호출해야 한다는 계약을 헤더와 호출부에 명시 | 10E |
| 5 | `WorkerGaugeSnapshot::inbox_queued_bytes`만 64-loop sampled인데 이름이 exact처럼 보였다 | `sampled_inbox_queued_bytes`로 rename + 주석. `poll_budget_stops`는 "stop event 횟수"라는 의미를 주석으로 고정 | 10A |
| 6 | ASan 반복 실행에서 active phase CPU residence가 약 10% 확률로 threshold를 넘었다(Poll 45.135 ms, Timers 9.249 ms) | 원인까지 조사해 **환경 freeze가 thread CPU로 계상되는 것**임을 확인하고, threshold 대신 (1) 계측 구간 정합성, (2) 환경 증인 기반 판정, (3) negative control을 도입 | D4, 10G, 10H |

**6라운드 — active phase CPU tail 조사 결과**

threshold는 바꾸지 않았다. 조사·증거·상수 근거는
[리포트 §10](./worker-runtime-quality-gates.md)에 전부 기록했다. 요약:

- worst active phase는 항상 **분할 불가능한 단일 항목 1개**와 같았다(Actors phase 6.6947 ms ≈ actor turn
  6.6810 ms). 같은 경로의 p50은 90~147 us다. 항목을 반복해 오래 돈 것이 아니다.
- 아무 일도 하지 않는 witness thread가 단일 gap 2.8~23.3 ms를 관측했고, 한 번은 10.04 ms gap 동안
  **CPU 6.87 ms를 계상받았다**. 이 환경은 실행하지 않은 시간을 thread CPU로 계상한다.
- outlier는 Poll/Timers/Actors/Writes에 무작위로 떨어졌고, `ru_nvcsw`/`ru_nivcsw`는 0이었다.
- 판정: 계획의 두 번째 갈래(분할 불가능한 단일 연산). 원인은 ASan allocator가 아니라 컨테이너/VM freeze이며
  ASan은 확률 증폭기다.
- 계약: active phase 상한 = `threshold + min(max(witness_max_gap - 2 ms, 0), 30 ms)`. gap이 32 ms를 넘으면
  그 window는 판정 불가로 보고 최대 3회 window만 다시 굴린다. threshold는 굴리지 않는다.
- negative control 2개. CPU spin: Inbox에서 150 ms CPU를 태우면 `threshold + SLACK_CAP`도 반드시 초과한다.
  **blocking wait**: Inbox에서 `sleep_for(150 ms)`하면 wall 153.968 ms인데 **CPU는 0.154 ms**라 CPU 상한만으로는
  통과한다. 따라서 sanitizer의 active phase wall 상한은 고정 slack 없이 `CPU threshold + witness slack`으로
  두고, Debug는 `ru_nvcsw > 0`으로 잡는다. `SANITIZER_POLL_WAIT_SLACK(400 ms)`은 설계상 blocking wait인
  PollWait 전용으로 분리했다.

---

## 사전 확인 사항 (계획에 영향을 준 사실)

아래는 전부 직접 grep/read로 확인했다.

| 사실 | 근거 | 영향 |
| --- | --- | --- |
| `DbClientConfig::max_queued_bytes`는 구현 전 **강제되지 않았다** | 설정은 [db_client.hpp:74](../include/snf/worker/db_client.hpp#L74), validator는 [db_client.cpp:137](../src/worker/db_client.cpp#L137), 최종 overflow-safe admission은 [db_client.cpp:1011](../src/worker/db_client.cpp#L1011)에 있다 | 10A에서 강제 완료 |
| **동명이인 함정**: `InboxLane`에도 `_max_queued_bytes`가 있고 이쪽은 **강제된다**([inbox.cpp:50](../src/worker/inbox.cpp#L50)) | 위 grep 결과 | `max_queued_bytes`를 grep하면 강제되는 것처럼 보인다. **DB 쪽과 무관한 별개 필드**임을 구현 시 혼동하지 말 것 |
| worker layer는 `snf/runtime/*`, `snf/server/*` include 금지 | [cmake/check_worker_layer.cmake:6](../cmake/check_worker_layer.cmake#L6), ctest `snf_worker_layer` | `snf::runtime::Distribution` 재사용 불가 → worker-local histogram을 새로 만든다 |
| `approximateQueuedBytes()`는 `InboxLane`에만 있음 | [include/snf/worker/inbox.hpp:79](../include/snf/worker/inbox.hpp#L79) | `WorkerInbox` 수준 집계 accessor를 새로 추가해야 inbox 상한을 관측 가능 |
| `TimerQueue::applicationTimerCount()`는 heap 선형 스캔 | [timer_queue.hpp:379](../include/snf/worker/timer_queue.hpp#L379)의 `for (const auto& entry : _heap)`. 반면 `applicationTimerBytes()`/`size()`는 O(1) 멤버 읽기 | 루프마다 샘플링 금지. `size()` + `applicationTimerBytes()`로 대체 |
| `pollTimeout()`은 반환형이 `optional`이지만 **모든 경로가 값을 반환**하고 `max_poll_timeout`으로 상한 | [src/worker/worker.cpp:855](../src/worker/worker.cpp#L855) | idle Worker도 ≤50ms마다 반복이 도는 것이 보장 → watchdog 임계값 유도의 근거 |
| `WorkerGroupConfig`에 `DbClientConfig` 자리가 없고 `worker(i)`는 const | [include/snf/worker/worker_network.hpp:75](../include/snf/worker/worker_network.hpp#L75) | load harness는 `WorkerGroup`이 아니라 단일 `Worker`로 DB 케이스를 구성한다 |
| ctest `LABELS`가 트리에 하나도 없음 | [CMakeLists.txt](../CMakeLists.txt) | 게이트별 재현 명령을 위해 `worker` / `load` / `mysql` 라벨을 새로 붙인다 |
| 공용 `connectClient(port, recv_buf)`가 필요하다 | 최종 helper는 [tests/socket_test_support.hpp:26](../tests/socket_test_support.hpp#L26) | slow-consumer 주입을 한 곳에 둔다 |

---

## 핵심 설계 결정

### D1. Cross-thread progress 발행 — 64비트 packed atomic 하나

새 헤더 `include/snf/worker/progress.hpp`:

```cpp
enum class WorkerPhase : std::uint8_t {
    Starting = 0, PollWait, Poll, Inbox, Timers, Db, Actors, Writes,
    ShutdownA, ShutdownB, ShutdownC, ShutdownD, Stopped,   // 13개, 4비트에 들어간다
};

class WorkerProgress final {
public:
    struct Sample { WorkerPhase phase; std::chrono::steady_clock::time_point entered_at; };
    void publish(WorkerPhase, std::chrono::steady_clock::time_point) noexcept;  // owner thread 전용
    [[nodiscard]] Sample sample() const noexcept;                               // 어느 thread에서든
private:
    alignas(64) std::atomic<std::uint64_t> _word{0};
};
```

- 비트 `[63:60]` = phase, `[59:0]` = `steady_clock` epoch 기준 마이크로초. 60-bit encoding 폭은
  2^60us ≈ 36,500년이다. 이는 packed field의 표현 폭이며 실제 플랫폼의
  `steady_clock::time_point::duration` 표현 범위를 늘린다는 뜻이 아니다.
- compile-time 보장을 명시한다:
  ```cpp
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free);  // 아니면 watchdog이 lock을 잡는다
  static_assert(static_cast<std::uint8_t>(WorkerPhase::Stopped) < 16);  // phase는 4비트 안
  ```
- 양쪽 모두 `memory_order_relaxed`. **이 word로 다른 데이터를 발행하지 않으므로** happens-before가 필요 없고,
  phase와 시각이 한 word에 있으므로 tearing이 불가능하다.
- **seqlock을 쓰지 않는 이유**: reader가 payload를 non-atomic으로 읽는 구조라 C++ 메모리 모델상 실제 race이고
  TSan이 그대로 보고한다. Thread ownership 게이트 자체가 "TSAN 0건"인데 그 게이트를 통과하려고 TSan
  suppression을 넣는 것은 자기모순이다.
- **전체 counter atomic mirror를 쓰지 않는 이유**: hot path의 `+=`가 전부 RMW가 되고,
  `metrics()`가 더 이상 `const WorkerMetrics&`를 반환할 수 없어 기존 assertion을 대량 수정해야 한다.
  watchdog이 필요한 것은 counter 값이 아니라 **마지막 진행 이후 경과 시간**뿐이다.
- **clock/OS sample 비용**: phase 전환 때 기존 wall clock과 `getrusage(RUSAGE_THREAD)`를 각각 한 번 읽는다.
  phase당 O(1)이며 CPU/context-switch 계측 비용 자체도 해당 phase residence에 포함된다. 실패/역행 sample은
  `thread_execution_sample_failures`로 드러내고 공식 gate는 0을 요구한다.
- accessor `Worker::progress()`는 cross-thread 안전으로 문서화하고, `metrics()` 계약 문단 바로 옆에 적는다.

**`enterPhase()`가 phase 전환 때 함께 기록하는 것** (owner thread, non-atomic):

```cpp
struct WorkerPhaseMetrics {           // phase마다 한 벌
    std::uint64_t entries{0};
    std::uint64_t budget_stops{0};
    std::chrono::nanoseconds max_residence{0};       // wall → fairness/장기 정지 안전망
    std::chrono::nanoseconds max_cpu_residence{0};   // CPU → active correctness
    std::chrono::nanoseconds max_entry_gap{0};       // wall → fairness
    std::uint64_t voluntary_context_switches{0};     // 실제 blocking 검출
    std::uint64_t involuntary_context_switches{0};   // scheduler 선점 진단
    std::chrono::nanoseconds max_wall_residence_cpu{0};
    std::uint64_t max_wall_residence_voluntary_context_switches{0};
    std::uint64_t max_wall_residence_involuntary_context_switches{0};
    std::chrono::steady_clock::time_point last_entered{};  // gap 계산용, 리포트에 노출 안 함
};
```

`enterPhase(next, now)`는 (1) **직전** phase의 wall/CPU/context-switch delta를 기록하고, (2) 같은
`max_residence` sample의 CPU/voluntary/involuntary 값을 함께 귀속하며, (3) `next.max_entry_gap`과 entry를
갱신하고, (4) progress word를 발행한다. 최초 진입은 gap 계산에서 제외한다. 각 max CPU와 max wall은 서로
다른 sample일 수 있으므로 직접 빼지 않고, 최대 wall의 원인 분석에는 같은 sample의 귀속 필드를 쓴다.

최종 No Worker blocking 계약은 다음처럼 역할을 분리한다.

- active phase: `max_cpu_residence < correctness_phase_threshold`.
- 일반 Debug(active sanitizer 없음) active phase: `voluntary_context_switches == 0`.
- active wall residence와 `max_entry_gap`: `fairness_threshold` 미만인 장기 정지/starvation 안전망.
- involuntary context switch: scheduler 선점 귀속용이며 그 자체는 실패 조건이 아니다.
- `PollWait`: 일반 Debug에서 wall `max_poll_timeout * 2` 미만.
- watchdog과 shutdown/fairness: 계속 wall-clock 기준.

Actor도 별도 wall-only 예외를 두지 않는다. `actor.turn_slice_ns` wall histogram은 분포 관측으로 유지하고,
correctness는 다른 active phase와 같은 **Actors phase CPU residence + voluntary context switch**로 판정한다.

### D2. Watchdog — `WorkerGroup` 소유, config opt-in, edge-triggered

새 `include/snf/worker/watchdog.hpp` + `src/worker/watchdog.cpp` (`snf_worker` 타겟에 추가).

```cpp
struct WorkerWatchdogConfig {
    std::chrono::milliseconds sample_interval{10};
    std::chrono::milliseconds phase_stall_threshold{500};
    std::chrono::milliseconds poll_wait_stall_threshold{0};  // 0이면 budgets에서 유도
};
struct WorkerStallReport { WorkerId worker; WorkerPhase phase; std::chrono::nanoseconds stuck_for; };
struct WorkerWatchdogMetrics {  // 전부 atomic, watchdog thread 소유
    std::atomic<std::uint64_t> samples_taken{0};
    std::atomic<std::uint64_t> active_stall_episodes{0};    // run loop phase에서의 stall — 게이트 판정용
    std::atomic<std::uint64_t> shutdown_stall_episodes{0};  // ShutdownA~D — 진단용, 게이트에 합치지 않는다
    std::atomic<std::uint64_t> longest_stall_ns{0};
    std::atomic<std::uint32_t> last_stall_phase{0};
};
class WorkerWatchdog final;  // std::thread 소유, ctor에 config/budgets/{WorkerId, const WorkerProgress*} 목록
```

- 판정: `stuck_for = now - sample().entered_at`. `Starting`/`Stopped`는 제외.
- `poll_wait_stall_threshold`는 유도값 `max_poll_timeout * 4 + phase_stall_threshold` (기본 700ms).
  근거: `pollTimeout()`이 최대 `max_poll_timeout`을 반환하는 것이 증명돼 있으므로 정상 idle Worker는
  ≤50ms마다 `PollWait`에 재진입한다. 그 몇 배를 넘으면 `epoll_wait`가 안 돌아온 진짜 stall이다.
- `phase_stall_threshold` 기본 500ms. 전체 phase budget 합이 3ms라 약 166배 여유 — Debug/ASan/TSan에서
  우는 watchdog은 없느니만 못하다. 대신 "watchdog이 실제로 발화하는가" 테스트는 임계값 50ms에
  400ms 블로킹을 주입해 좁은 경로를 덮는다.
- **무엇이 edge-triggered이고 무엇이 아닌지를 나눈다.** 전부 edge로 하면 `longest_stall_ns`가 최초 감지
  시점 값(≈threshold)에 머물러 "400ms 멈췄다"를 못 잰다:
  - `stall_episodes`, callback → **edge-triggered** (`false→true` 전이에서만). 아니면 episode 수가
    `sample_interval`의 함수가 되어 "정확히 1건"을 테스트할 수 없다.
  - `longest_stall_ns` → stalled인 **매 sample에서 atomic max**. 이래야 `>= 300ms` assert가 성립한다.
  - `last_stall_phase` → stalled인 **매 sample에서 현재 phase를 저장**(덮어쓰기). `max()` 개념이 아니다.
- 발화 시: counter + 선택 콜백. **throw/abort/requestStop 없음** (자동 대응은 이 단계 항목이 아니다).
  `progress()` 외에는 Worker의 어떤 것도 건드리지 않는다.
- `WorkerGroupConfig::watchdog`(`std::optional<WorkerWatchdogConfig>`)로 opt-in. worker thread **뒤에** 시작하고
  join **뒤에** 정지해 shutdown phase A~D도 감시 범위에 넣는다. 멤버는 `_workers` **뒤에** 선언하고
  소멸자에서 명시적으로 stop한다 (`const WorkerProgress*`를 들고 있으므로 순서가 뒤집히면 UAF).
- 단독 `Worker`도 감시할 수 있도록 별도 클래스로 둔다 — 결정적 테스트가 정확히 이 형태를 쓴다.

### D4. Threshold — 진단용과 correctness용을 분리하고 둘 다 budget에서 유도

watchdog의 여유 threshold(250~500ms)를 그대로 게이트 상한으로 쓰면 **100ms짜리 동기 block이 통과한다.**
둘은 목적이 다르므로 분리한다.

| 종류 | 값 | 용도 |
| --- | --- | --- |
| **진단 threshold** (watchdog) | phase 500ms / PollWait `max_poll_timeout*4 + 500ms` | "명백히 멈췄다"만 잡는다. sanitizer에서 울지 않는 것이 목적 |
| **active CPU correctness threshold** (게이트) | phase별 `budget.max_duration * K + item_allowance` | 실제 owner-thread 실행 비용 판정. 훨씬 타이트하다 |
| **active wall fairness threshold** (게이트) | `(phase budget 합 + max_poll_timeout) * K` | 선점 포함 장기 정지/starvation 보조 안전망 |
| **blocking signal** (게이트) | 일반 Debug active `ru_nvcsw == 0` | sleep/futex/blocking syscall 같은 실제 양보 검출 |

**threshold는 load test를 돌리기 전에 고정한다** (피드백 3). 정하지 않고 실행하면
"4.7ms 나왔네 → 5ms로 하자"는 **test-to-pass tuning**이 된다. 공식:

```text
active_cpu_correctness_threshold = configured_phase_budget * preset_multiplier + max_single_item_overshoot
```

`max_single_item_overshoot`도 **사전에 근거를 갖는다.** budget이 작업 항목 **사이에서만** 검사되므로
분할 불가능한 한 항목이 budget을 넘길 수 있고, 그 "한 항목"이 무엇인지가 phase마다 다르다.
**구현 시 다시 정하지 않도록 숫자까지 지금 박는다** — 이 값들은 `budget.hpp` 옆에 상수로 두고,
바꾸려면 별도 calibration 커밋에서 근거와 함께 바꾼다 (gate 실행 중 변경 금지):

| phase | budget | 분할 불가능한 항목 | `single_item_allowance` |
| --- | ---: | --- | ---: |
| Poll | 500us | connection 1개 read + decode | **500us** |
| Inbox | 250us | envelope 1건 dispatch | **500us** |
| Timers | 250us | timer callback 1건 + bookkeeping | **500us** |
| Db | 500us | DB step 1회 | **500us** |
| Actors | 1ms | **actor turn 1회** (가장 큰 단일 단위) | **2ms** |
| Writes | 500us | send 시도 1회 | **500us** |

`preset_multiplier`도 **게이트 실행 전에 고정**한다: Debug `8`, ASan/UBSan `20`, TSan `40`.

Actors도 같은 식을 쓴다. `actor.turn_slice_ns.max`는 wall 분포 관측값이며 correctness gate로 직접 쓰지 않는다.
Actor blocking은 Actors phase의 CPU residence와 voluntary context switch로 다른 active phase와 일관되게 판정한다.

`timeout_overshoot_limit`(10G의 DB latency band 상한) **= `fairness_threshold`와 같은 유도식을 쓴다.**
근거: DB timeout 판정은 timers phase에서 일어나므로 "얼마나 늦게 감지될 수 있는가"의 상한은
곧 "timers phase가 다시 돌기까지의 최대 간격" = `(모든 phase budget 합 + max_poll_timeout) * K`다.
기본 budget·Debug 기준 약 424ms.

실행 순서는 반드시 (1) calibration 실행, (2) CPU/wall/context-switch 측정 확인, (3) 필요하면 근거를 별도
기록, (4) threshold 고정, (5) 깨끗한 공식 gate 재실행이다. 공식 gate 중에는 threshold를 바꾸지 않는다.

- `PollWait`은 **제외하지 않는다.** 일반 Debug에서 별도 wall 상한 `max_poll_timeout * 2`를 확인한다
  (`pollTimeout()`이 `max_poll_timeout`으로 상한된다는 검증된 사실에서 유도).
- `fairness_threshold` = `(모든 phase budget 합 + max_poll_timeout) * K`. 기본 budget에서
  phase 합 3ms + 50ms = 53ms이므로 Debug 기준 약 424ms. 지속 부하 구간(`hasRunnableWork()`가 참이라
  poll timeout이 0)에서는 실질적으로 `3ms * K`가 상한이 된다.
- **active-loop stall과 shutdown을 분리 판정한다** (피드백 7). 정상 shutdown phase A~D는 quiescence 대기로
  250ms를 넘길 수 있으므로 shutdown episode를 `totalStallEpisodes()`에 합치면 거짓 실패가 난다.
  watchdog은 `WorkerPhase`가 `ShutdownA~D`인 stall을 **별도 카운터** `shutdown_stall_episodes`로 세고,
  게이트는 `active_stall_episodes == 0`을 보며, shutdown의 시간 준수는 10E의 deadline 필드로 판정한다.

ASan/UBSan과 TSan은 allocator와 synchronization runtime을 owner thread에 삽입하고 OS 재스케줄 시점도
바꾼다. calibration에서 sanitizer 자체의 active voluntary switch와 100ms를 넘는 PollWait wall이 재현됐으므로
raw `ru_nvcsw`/PollWait wall의 pass/fail 권위는 일반 Debug에 둔다. sanitizer에서는 값을 숨기지 않고
진단값으로 출력하며, active CPU, wall fairness, 기능 assertion과 sanitizer report는 그대로 공식 gate다.
TSan suppression은 사용하지 않는다.

계측 계약 자체는 deterministic test로 검증한다: CPU spin은 CPU residence를 늘리고 voluntary switch는 0,
sleep/blocking handler는 voluntary switch를 만들며 CPU는 wall보다 작아야 한다. 정상 idle/load의 active
voluntary switch는 0이다. 마지막으로 Worker와 competitor를 같은 CPU에 고정한 선점 테스트는 wall이
correctness threshold를 넘더라도 CPU는 threshold 이하, voluntary 0, involuntary > 0임을 확인한다.

### D3. Latency — worker-local, owner-thread 전용 histogram 4개

새 `include/snf/worker/latency_histogram.hpp`: `LatencyHistogram` (non-atomic `std::uint64_t` bucket,
`Distribution`과 같은 bucketing 수식, `record(nanoseconds)` / `snapshot() -> {count,sum,max,p50,p95,p99}`,
복사 가능 약 2KB). `snf::runtime::Distribution`은 layer check가 금지하고, 497개 atomic에 non-copyable이라
`WorkerMetrics` 안에 넣을 수도 없다. 중복은 의도적이며 `snf_runtime`이 사라지는 11단계에서 정리한다.

기록 대상 4개, 각각 근거가 있는 것만:

1. `WorkerActorMetrics::turn_slice_ns` — Actor turn wall 분포의 관측값. correctness의 1차 증거는
   Actors phase CPU/context-switch다. 기존 3개 지점
   ([worker.cpp:1350](../src/worker/worker.cpp#L1350),
   [:1424](../src/worker/worker.cpp#L1424), [:1636](../src/worker/worker.cpp#L1636))에 `record()` 한 줄씩.
   **`total_slice_duration_ns`와
   `max_slice_duration`은 그대로 둔다** (기존 테스트가 읽는다).
2. `WorkerMetrics::loop_iteration_ns` — 런타임 한 바퀴 비용. stall 임계값의 sanity check 근거.
3. `DbClientMetrics::operation_latency_ns` — submit→terminal. `Impl::Queued`/`InFlight`에
   `submitted_at` 필드를 추가한다. **이것이 커지는 동안 active phase CPU가 상한 미만이고 voluntary switch가
   0이라는 조합이 "느린 DB에도 Worker가 블록되지 않는다"의 정량적 증거다.** `turn_slice_ns`는 보조 분포다.
   **기록 계약**: admission을 통과한(accepted) operation은 **success / timeout / error / cancel 중
   어느 terminal outcome에서든 정확히 한 번** 기록한다 (피드백 4). success만 기록하면 느린 DB 시나리오에서
   대부분이 timeout으로 끝나 histogram이 비고, 게이트가 근거를 잃는다.
   `Rejected`(admission 실패)는 operation이 생기지 않았으므로 기록하지 않는다.
   Debug에서 `operation_latency_ns.count == operations_completed + operations_failed`를 검사해
   누락·이중 기록을 잡는다.
4. `DbClientMetrics::queue_wait_ns` — submit→dispatch. 같은 `submitted_at`으로 공짜이며,
   "서버가 느린 것"과 "우리 admission queue가 병목인 것"을 구분한다.

제외: phase별 latency histogram(6개 추가 12KB — Fairness는 count 질문이라 phase entry counter로 충분),
connection write-drain latency(queued frame마다 timestamp가 필요해 Memory bound가 줄이려는 바로 그 버퍼에
메모리를 더한다. `epollout_waits`/`epollout_resumes`/`write_budget_stops` + write high-water로 대체).

---

## 서브 스텝

| 스텝 | 커밋 | 닫는 테스트 |
| --- | --- | --- |
| 10A-pre | `style: clang-format the files stage 10 touches` | 기존 스위트 무변경 |
| 10A | `feat(worker): add bounded gauges, high-water marks and DB queue byte accounting` | 신규 `tests/worker_metrics_test.cpp` |
| 10B | `feat(worker): publish per-phase progress for cross-thread observation` | 신규 `tests/worker_progress_test.cpp` |
| 10C | `feat(worker): add owner-thread latency histograms` | 신규 `tests/worker_latency_histogram_test.cpp` |
| 10D | `feat(worker): detect worker stalls with a progress watchdog` | 신규 `tests/worker_watchdog_test.cpp` |
| 10E | `feat(worker): instrument graceful shutdown phases and deadlines` | `tests/worker_actor_test.cpp` shutdown 케이스 확장 |
| 10F | `feat(worker): observe the WorkerGroup join deadline` | `tests/worker_watchdog_test.cpp` 추가 케이스 |
| 10G | `test(worker): add the bounded load scenario harness` | 신규 `tests/worker_load_scenario_test.cpp` (종합 압력) + deterministic **slow-consumer**·**stale** 테스트 (의미 증명은 여기서 담당) |
| 10H | `docs: record the stage 10 quality gate runs` | 없음 (실제 실행 결과를 붙여넣는다) |

의존: 10A→10C, 10B→10D·10F, (10A+10B+10C+10D+10E)→10G, 전부→10H. 10A와 10B는 서로 독립이라 순서 교환 가능.

### 10A-pre — 포맷 선행 커밋

트리가 clang-format clean이 아니므로, 이번 단계가 건드릴 파일을 **먼저** 별도 `style:` 커밋으로 정렬한다
(`81a2882` 선례). 대상: `include/snf/worker/{worker,budget,actor,worker_network,db_client,worker_group,inbox,timer_queue}.hpp`,
`src/worker/{worker,worker_group,db_client,inbox}.cpp`,
`tests/{worker_test_main,worker_loop_test,worker_actor_test,worker_network_integration_test,worker_db_client_test}.cpp`,
`tests/socket_test_support.hpp`. 호스트에 clang-format이 없으므로 Docker 안에서
`clang-format --dry-run -Werror`로 확인한다.

### 10A — gauge, high-water mark, DB queue byte accounting

요구사항이 "counter, **gauge**, high-water mark, latency snapshot"을 모두 명시하므로 current gauge를
별도 타입으로 공개한다 (피드백 부수). `WorkerHighWaterMarks`는 이 gauge를 샘플링해 갱신한다.

```cpp
struct WorkerGaugeSnapshot {   // 샘플 시점의 현재값. sampleGauges()가 채운다
    std::size_t connections{0}, actors{0}, loading{0}, ready_actors{0};
    std::size_t mailbox_messages_total{0};  std::uint64_t mailbox_bytes_total{0};
    std::size_t timer_entries{0};           std::uint64_t application_timer_bytes{0};
    std::uint64_t inbox_queued_bytes{0};
    std::size_t db_queued_operations{0};    std::uint64_t db_queued_bytes{0};
    std::size_t db_in_flight{0};
};
```

`include/snf/worker/worker.hpp`에 `WorkerMetrics` 옆으로:

**exact HWM과 sampled HWM을 이름으로 구분한다** (피드백 5). 64반복마다 읽은 값은 엄밀히는 high-water가
아니다 — `10KB → 800KB(순간 peak) → 30KB` 사이에 샘플하면 30KB가 기록된다. 섞어 두면 리포트를 읽는 사람이
관측 강도를 오해한다.

```cpp
struct WorkerHighWaterMarks {
    // --- mutation-site exact: 갱신 지점에서 세므로 순간 peak을 놓치지 않는다 ---
    std::size_t connection_read_buffer_bytes{0}, connection_write_queued_bytes{0};
    std::size_t db_queued_operations{0};     std::uint64_t db_queued_bytes{0};
    std::size_t db_in_flight{0};

    // --- sampled(매 반복): 한 반복 안에서 올랐다 내려가면 놓친다 ---
    std::size_t sampled_connections{0}, sampled_actors{0}, sampled_loading{0}, sampled_ready_actors{0};
    std::size_t sampled_mailbox_messages_total{0};  std::uint64_t sampled_mailbox_bytes_total{0};
    std::size_t sampled_timer_entries{0};           std::uint64_t sampled_application_timer_bytes{0};

    // --- sampled(64반복마다): 관측 밀도가 가장 낮다 ---
    std::uint64_t sampled_inbox_queued_bytes{0};
};
```

**매 반복 샘플링도 exact가 아니다.** 예컨대 actor는 한 반복의 actors phase 안에서 생겼다 사라질 수 있다.
`sampled_` 접두사를 붙여 관측 밀도를 이름에 드러내고, 리포트에서도 exact 블록과 나눠 적는다.
cap을 절대 못 넘는다는 증명은 어차피 HWM이 아니라 admission 테스트가 한다.

- 새 private `Worker::sampleGauges()`를 루프 본문 끝(`flushWrites` 뒤)과 shutdown phase 경계마다 호출.
  - **Tier 1 (매 반복, 전부 O(1))**: `_connections->activeCount()`, `_actors->activeCount()`,
    `_loading_count`, `_total_mailbox_messages`, `_total_mailbox_bytes`, `_ready_queue->size()`,
    `_timers.size()`, `_timers.applicationTimerBytes()`, `_db->queuedCount()`.
  - **Tier 2 (64반복마다)**: inbox queued bytes. 새 `WorkerInbox::approximateQueuedBytes()` /
    `maxQueuedBytesTotal()`을 추가해 lane 합을 낸다.
  - **절대 샘플링하지 않음**: `TimerQueue::applicationTimerCount()` (heap 선형 스캔).
- **mutation-site high-water** (스캔이 아니라 갱신 지점에서 기록 — O(n) 회피 + 순간 피크 포착):
  - `connection_write_queued_bytes`: `sendLocal`의 `appendFrame` 성공 직후
    ([worker.cpp:2637](../src/worker/worker.cpp#L2637))
  - `connection_read_buffer_bytes`: `readConnection`의 `decoder().push()` 직후
    ([worker.cpp:1133](../src/worker/worker.cpp#L1133))
  - `db_*`: `DbClient` 내부 `assign` / `tryStart`의 `queue.push_back`
    ([db_client.cpp:1018](../src/worker/db_client.cpp#L1018))
- **`max_queued_bytes` 강제 — 증가/감소 전 경로를 계약으로 못박는다** (사용자 피드백 7).
  `push_back` 지점만 세면 누수/음수로 조용히 틀어진다.

  | 사건 | `queued_bytes` |
  | --- | --- |
  | enqueue 성공 | `+= queuedFootprint(request)` |
  | dispatch (queue → in-flight) | `-=` |
  | queued timeout | `-=` |
  | cancel / shutdown drain | `-=` |
  | admission rejection | 변화 없음 (애초에 넣지 않는다) |

  `queuedFootprint(const DbRequest&)`를 명시적으로 둔다. `SavePlayer`처럼 `vector`를 소유하는 요청은
  `sizeof(DbRequest)`만으로는 실제 점유를 못 세므로 **owned dynamic payload를 포함**해 계산한다.
  - **logical이 아니라 allocated byte를 센다.** Memory bound를 실제 heap 점유 의미로 말하려면
    `vector`는 `size()`가 아니라 **`capacity() * sizeof(T)`** 가 맞다 (피드백 부수). 이 선택을 헤더 주석과
    리포트에 명시한다.
  - **Debug 불변식은 합 검증까지 한다**: `queued_bytes == Σ queuedFootprint(queue[i])`.
    `queued_bytes == 0 iff queue.empty()`만으로는 두 항목 중 하나의 감소를 틀려도 queue가 비지 않으면
    통과한다.
  - **admission은 overflow-safe하게 비교한다**: `queued_bytes + footprint > cap`이 아니라
    `footprint > cap || queued_bytes > cap - footprint`.
  - op 수와 byte 둘 다 확인하고 초과 시 `Rejected`를 반환한다.
- phase counter: `poll_budget_stops`([worker.cpp:982](../src/worker/worker.cpp#L982)) 추가.
  나머지 phase별 수치는 10B의 `WorkerPhaseMetrics`가 담당한다 (D1 참조).
- 신규 `tests/worker_metrics_test.cpp` + `run_worker_metrics_tests()`를 `tests/worker_test_main.cpp`와
  `CMakeLists.txt:256` 블록에 등록. **Memory bound 게이트의 1차 근거가 여기다** (피드백 5):
  자원별로 (1) cap 직전까지 accepted, (2) cap 초과는 rejected, (3) accounting 불변식 유지를
  결정적으로 확인한다. 대상은 inbox lane bytes, mailbox(개수·byte), ActorTable, Loading,
  application timer bytes, DB queue(op·byte), connection read/write buffer.
  기존 테스트가 이미 덮는 자원은 중복하지 않고 빠진 것만 채운다.

### 10B — phase progress 발행

D1의 `progress.hpp`를 추가하고, `Worker`에 `_progress`/`WorkerPhaseMetrics phases[N]` 멤버와
`progress()` accessor, private `enterPhase(WorkerPhase, TimePoint)`를 넣는다. 발행 지점은 run 루프 7곳
([src/worker/worker.cpp:130](../src/worker/worker.cpp#L130)) + shutdown 4곳.
`Actors`/`Writes`는 각각의 early return **위로** clock 읽기를 끌어올린다 —
early return 이전에 진입을 기록해야 "그 phase에 계속 도달했다"가 측정된다.

`tests/worker_progress_test.cpp`: (1) pack round-trip, 60비트 encoding 경계와 실제 `steady_clock` 표현 범위의
분리 포함, (2) **실행 중인 Worker를 다른
thread에서 관측** — 지금 [tests/worker_loop_test.cpp:17](../tests/worker_loop_test.cpp#L17)이 문서화한
"핸들러 안 atomic 우회" 워크어라운드를 대체한다, (3) `run()` 반환 후 `Stopped`,
(4) CPU spin/sleep/idle의 CPU와 voluntary switch, (5) same-CPU 선점의 wall/CPU/voluntary/involuntary 귀속,
(6) 인위적으로 한 phase를 지연시켰을 때 해당 wall residence와 **다른** phase의 `max_entry_gap`이 함께
커지는지 확인한다.

### 10C — latency histogram

D3의 `latency_histogram.hpp` 추가 후 4개 histogram 배선.

`tests/worker_latency_histogram_test.cpp`: bucket histogram의 p50/p95/p99는 **exact percentile이 아니라
bucket 근사**다. 따라서 정렬된 참조 벡터와 **값을 동일 비교하지 않는다.** 대신
"참 percentile이 반환된 bucket의 `[lower, upper)` 범위 안에 있는가"와 상대오차 상한(bucket 해상도에서
유도)을 검증한다. `count`/`sum`/`max`는 정확해야 하므로 그것만 정확 비교한다.

### 10D — watchdog

D2 구현. `WorkerGroupConfig`에 `watchdog` 필드 추가 + `isValid()` 확장.
`tests/worker_watchdog_test.cpp` 3케이스:

1. `test_watchdog_reports_zero_stalls_for_an_idle_worker` — `active_stall_episodes == 0` **그리고**
   `samples_taken > 0` (한 번도 안 돈 watchdog도 0을 보고한다).
2. `test_watchdog_fires_on_a_blocking_event_handler` — `setEventHandler`가 400ms 자고, 임계값 50ms,
   `sample_interval` 5ms. `active_stall_episodes == 1`(edge), `last_stall_phase == Inbox`,
   `longest_stall_ns >= 300ms`(stalled 매 sample `max()` 갱신 덕분에 성립 — D2). 소켓·DB 없이 결정적.
3. `test_watchdog_fires_in_the_actor_phase` — actor turn이 400ms 자고 `last_stall_phase == Actors`.
   phase 귀속이 맞는지를 증명한다.

(네 번째 "느린 DB에도 stall 0"은 MySQL 링크가 필요해 10G의 load 시나리오에서 닫는다.)

### 10E — shutdown 계측

**시점을 분리한다.** lifecycle이 `A → B → C → D → _db->shutdown() → run() 종료`이므로
`phase_d` record 안에 DB 잔여를 넣으면 그 값은 **DB shutdown 이전 상태**다. 별도 snapshot으로 나눈다:

```cpp
struct WorkerResourceSnapshot {
    std::size_t connections{0}, actors{0}, blocked_actors{0}, loading{0};
    std::uint64_t inbox_bytes{0};
    std::size_t timer_entries{0};  std::uint64_t application_timer_bytes{0};
    std::size_t db_in_flight{0}, db_queued{0};
};
struct WorkerShutdownPhaseRecord {
    bool entered{false};  std::chrono::nanoseconds duration{0};  bool deadline_hit{false};
    WorkerResourceSnapshot remaining{};   // 그 phase를 나온 시점
};
struct WorkerShutdownMetrics {
    // Actor quiescence(A~D)와 DB backend 종료는 서로 다른 예산을 갖는 별개 단계다.
    std::chrono::nanoseconds actor_configured_timeout{0}, actor_phases_duration{0};
    std::chrono::nanoseconds db_configured_timeout{0},    db_shutdown_duration{0};
    std::chrono::nanoseconds total_duration{0};
    bool actor_deadline_exceeded{false}, db_deadline_exceeded{false};
    WorkerShutdownPhaseRecord phase_a{}, phase_b{}, phase_c{}, phase_d{};
    WorkerResourceSnapshot final_resources{};   // _db->shutdown() 반환 뒤
    std::uint64_t forced_connection_closes{0}, forced_actor_removals{0}, forced_ready_queue_drops{0};
};
```

판정도 시점을 나눈다: **Actor/connection 자원은 `phase_d.remaining`에서, DB 자원은 `final_resources`에서**
확인한다 (아래 잔여 자원 표).

**DB shutdown deadline 분리** (사용자 피드백 2). 8단계에서 DB backend 종료는 Actor quiescence와 분리된
별도의 deadline-bounded 단계로 정해졌고, 실제 코드도 `_db->shutdown(_poller)`를 quiescence **뒤에**
둔다([src/worker/worker.cpp:2816](../src/worker/worker.cpp#L2816)). 따라서 하나의 `configured_timeout`으로
전체를 재면 게이트가 틀린다. `DbClientConfig`에 `shutdown_timeout`(기본 1000ms)을 추가하고 예산을 나눈다:

```text
phase A~D duration      <= worker_shutdown_timeout
db_shutdown_duration    <= db_shutdown_timeout
total_duration          <= worker_shutdown_timeout + db_shutdown_timeout
```

**`db_shutdown_timeout`은 측정용이 아니라 실제로 강제되는 deadline이다** (피드백 2).
`_db->shutdown(_poller)`를 부르고 반환 뒤 경과시간을 재서 `db_deadline_exceeded`를 세우는 구조는
**"늦었다는 것을 사후에 아는 것"이지 bounded 반환의 보장이 아니다.** 시그니처를
`shutdown(Poller&, TimePoint deadline)`으로 바꾸고 deadline을 **내부 connect/read/write/fetch/cancel/drain
루프까지 전달**한다.

**다만 계약을 두 층으로 나눈다.** "deadline 안에 무조건 반환"은 C API의 마지막 cleanup까지 선점 가능하다는
전제가 없으면 지킬 수 없는 약속이다:

| 층 | 계약 |
| --- | --- |
| **async shutdown progress** | **deadline-bounded.** 만료 시 async 진행을 중단하고 socket을 shutdown/close 가능한 상태로 전환한다 |
| **final driver teardown** | deadline 밖. **8단계 driver conformance에서 bounded임이 확인된 primitive만** 사용한다 |

`mysql_close()`가 장시간 block할 가능성을 "그럴 리 없다"고 전제하지 않는다 — 목표 아키텍처의 핵심이
Worker thread에서 blocking API를 금지하는 것이기 때문이다. 따라서 10H 리포트에
**"MySQL final teardown boundedness: 8단계 conformance 근거 / 또는 이번 라운드 미측정"** 을 명시한다.
근거가 없으면 없다고 적는다.

`db_deadline_exceeded`는 async 층의 강제가 실제로 걸렸는지를 기록하는 부산물이다.

10F의 group deadline도 이 합(= Worker 전체 종료 예산)을 기준으로 잡는다.

`WorkerMetrics`에 `shutdown{}`을 추가한다. 기존 `shutdown_*` counter 5개는 **그대로 둔다** (테스트가 읽는다).
새 private `captureShutdownPhase(record, entered_at)`가 Tier-1 게이지 + 새 `blockedActorCount()`로 잔여 자원을 채운다
(`blockedActorCount()`는 O(actors)라 phase 경계 4회만 호출하고 `sampleGauges()`에는 절대 넣지 않는다).
phase B/C는 이미 `now() < deadline`을 검사하므로 종료 시 `deadline_hit`만 세팅하면 된다.
phase D에서 강제 정리 3종을 센다. `_db->shutdown()`은 위에서 정한 대로 **시그니처를 바꿔 deadline을
인자로 받고 내부 루프까지 전달**한다 (측정만 하는 것이 아니다).
`tests/worker_actor_test.cpp`의 기존 shutdown 케이스를 확장하고, 아주 짧은 timeout으로
`phase_b.deadline_hit == true` / `forced_actor_removals > 0`을 확인하는 케이스와,
**DB만 응답하지 않을 때** `db_deadline_exceeded == true`이면서
`actor_deadline_exceeded == false`인 케이스를 추가한다 — 두 예산이 실제로 분리돼 있음을 증명한다.

### 10F — WorkerGroup join 관측

`WorkerGroupConfig::group_shutdown_grace{1000ms}` 추가. 그룹 deadline =
`max(worker_shutdown_timeout + db_shutdown_timeout) + group_shutdown_grace` (10E에서 나눈 합).

**완료 판정은 `progress == Stopped`가 아니라 실제 thread 종료로 한다** (사용자 피드백 1).
`Stopped`를 발행한 **뒤** `run()`이 완전히 반환하기 전에 문제가 생기면 group이 "끝났다"고 오판한다.
`workerMain()`([src/worker/worker_group.cpp:261](../src/worker/worker_group.cpp#L261))의 wrapper에서
`worker.run()`이 실제로 반환한 직후 `_thread_exited[i].store(true, release)`를 세우고,
deadline 판정은 **오직 이 플래그**를 본다. `progress()`는 "어디에서 멈췄는지"를 알려주는 **진단용**으로만 쓴다
(초과 기록의 `phase` 필드).

**deadline의 시작점은 `join()` 호출 시점이 아니라 `requestStop()` 시점이다.** `requestStop()`에서
`_stop_requested_at`을 기록하고 `join()`이 그것을 기준으로 잰다. 그렇지 않으면 stop과 join 사이 지연이
예산을 공짜로 늘려준다.

**알고리즘 순서를 고정한다** (피드백 1). `std::thread::join()`을 먼저 부르면 **첫 번째로 멈춘 thread에서
영원히 막혀 deadline 시점에 `_thread_exited[]`를 검사할 기회 자체가 없다.** 반드시 이 순서다:

```text
1. requestStop() — _stop_requested_at을 최초 1회만 기록 (여러 번 호출돼도 덮어쓰지 않는다)
2. deadline = _stop_requested_at + group_budget 까지, _thread_exited[] 만 wait/poll
   (thread::join()을 여기서 부르지 않는다)
3. deadline 순간, 아직 exit하지 않은 worker의 snapshot을 뜬다
   — {WorkerId, progress().sample().phase(진단용), stuck_for} → _join_overruns
   — watchdog stall handler가 있으면 호출
4. 그 뒤에 비로소 모든 thread::join()을 무조건 수행한다
```

`detach()`는 `~WorkerGroup` 이후 `_workers[i]` 접근으로 UAF이고 `std::terminate()`는 보고 전에 프로세스를
죽인다. **기록한 뒤 계속 블록**하는 것이 진단과 안전을 동시에 만족하는 유일한 선택이며,
CTest `TIMEOUT`이 바깥쪽 백스톱이다.

**`requestStop()` 없이 `join()`이 불린 경우** (기존 API상 가능): deadline의 기준점이 없으므로
`join()` 진입 시점에 `requestStop()`을 스스로 호출한 것으로 간주해 그때를 기준점으로 삼는다.
이 규칙을 `join()` 주석에 계약으로 적는다.

### 10G — bounded load scenario

선행: [tests/socket_test_support.hpp:26](../tests/socket_test_support.hpp#L26)의 공용 `connectClient`에
`int receive_buffer_size = 0` 기본 인자를 추가하고 통합 테스트의 로컬 사본을 지운다.

신규 `tests/worker_load_scenario_test.cpp` → 신규 타겟 `snf_worker_load_scenario_tests`
(`snf_worker_game_runtime` + `PkgConfig::MySQL` + `Threads::Threads`, `snf_worker_db_client_tests`와 동일 구성).
ctest 2개: `snf_worker_load_stub`(`--stub`, TIMEOUT 90, 항상 실행) /
`snf_worker_load_mysql`(`--mysql`, TIMEOUT 120, `SKIP_RETURN_CODE 77`).

**`snf::load::LoadClient`를 재사용하지 않는 이유**: (1) `Zone`/`Battle` 시나리오가
`Authenticate → EnterZone → RoomJoin → BattleStart`를 요구하는데 worker 경로는 아직 `Ping`/`Pong` +
player DB 왕복만 서빙한다 — 그 프로토콜을 붙이는 것이 11단계다. (2) `LoadClientResult`가 요청마다
`std::vector<duration>`을 누적해 **메모리 상한을 검증하는 테스트가 스스로 무한 증가**한다.
(3) 자체 thread/타이밍을 써서 high-water를 읽을 정확한 정지 시점을 만들 수 없다.
`LoadClient`는 11단계의 end-to-end 측정 도구로 남긴다.

토폴로지: DB 케이스는 **단일 `Worker`** ([tests/worker_db_client_test.cpp:692](../tests/worker_db_client_test.cpp#L692)와
동일 배선 — 프로덕션 API 변경 0). 두 번째 케이스로 DB 없는 2-worker `WorkerGroup`을 돌려 cross-worker inbox 압력과
group join deadline을 덮는다.

4종 동시 주입, 전부 bounded:

0. **probe client 1개** — 부하 클라이언트와 **분리된 저율 probe**. 초당 몇 회만 `Ping`을 보내고 `Pong`을
   확인한다. Behavior parity는 **이 probe로만** 판정한다 (사용자 피드백 6): bounded overload를 일부러
   일으키면서 모든 부하 client의 모든 `Ping`에 `Pong`을 요구하면 두 계약이 서로 충돌한다.
1. **client I/O** — 소켓 32개, 각자 writer thread에서 고정 간격으로 `Ping` 전송, `atomic<bool>`로 정지.
   load client socket, frame, thread는 모두 Worker 시작 전에 만들고 atomic start gate에서 기다린다.
   `max_accepts_per_poll = 1`로 accept burst를 active Poll phase에 만들지 않는다.
2. **slow consumer** — 그중 4개는 작은 `SO_RCVBUF`로 붙고 **절대 `recv()`하지 않는다.**
   여기서는 종합 압력의 일부일 뿐이고, **slow-consumer 의미 증명은 아래 별도 결정적 테스트가 담당한다**
   (피드백 4). 종합 load에서 `epollout_waits > 0`을 correctness 증명으로 강제하지 않는다 —
   3초 안에 kernel send buffer가 차기를 "운 좋게" 기다리는 구조라 게이트로서 약하다.
3. **hot Actor** — 나머지 28개가 **같은 `ActorKey`**를 향한다. mailbox 하나가 경합점.
4. **slow DB** — `--stub`: 임시 listener에서 port를 얻은 뒤 **Worker 시작 전에 listener를 닫아** 즉시
   connection-refused가 되는 `DbClientConfig`, `operation_timeout = 200ms`. handshake를 보내지 않는 TCP peer는
   driver teardown wait를 active Db phase에 섞으므로 사용하지 않는다. `--mysql`: 실서버 +
   `connection_count = 1`, `max_queued_operations = 8`.
   `setPlayerLoadEnabled(true)`로 첫 메시지마다 activation load가 걸려 DB가 hot path에 들어간다.

   **주의 — hot actor 케이스는 DB queue를 포화시키지 못한다** (피드백 4). 28개 client가 **같은
   `ActorKey`**를 때리면 아키텍처상 최초 요청 하나만 `Loading + ActivationLoad`를 만들고 나머지는
   같은 slot의 mailbox에 쌓인다. 즉 **DB operation은 1건**이다. 따라서:
   - 부하 실재 증거를 `queued_timeouts > 0`으로 잡으면 **거짓 전제**다.
     `operation_timeouts > 0` **AND** `operation_latency_ns.max`가 `operation_timeout`에 근접함으로 바꾼다.
   - **DB queue 포화(=`db_queued_*` high-water가 의미를 갖는 것)는 별도 케이스**로 분리한다:
     서로 다른 `ActorKey` 수백 개를 동시에 활성화해 activation load를 동시 다발로 만든다.
     이 케이스에서만 `db_queued_operations` / `db_queued_bytes` high-water와 `submit_rejections`를 본다.

**load harness 자체도 bounded여야 한다** (피드백 6). writer thread가 blocking `send()`에, probe가
blocking `recv()`에 걸리면 `atomic<bool>`을 뒤집어도 종료되지 않고, 그러면 **Worker가 아니라 test client
때문에** CTest timeout이 난다 — 게이트가 거짓 실패한다. lifecycle을 고정한다:

- client socket은 **non-blocking 또는 `SO_SNDTIMEO`/`SO_RCVTIMEO` 기반**으로 연다.
- client socket/frame/thread는 Worker 시작 전에 모두 준비하고 start flag 이후에만 traffic을 보낸다.
- 종료 시퀀스: `stop_flag = true` → **모든 client fd에 `::shutdown(fd, SHUT_RDWR)`** 로 블록된 syscall을
  깨움 → writer/probe thread join → socket close.
- probe의 `Pong` 대기는 **absolute deadline**을 갖는다 (무기한 대기 금지).
- harness thread join 자체에도 상한을 두고, 초과 시 테스트를 명시적으로 실패시킨다
  (조용히 매달려 timeout이 나는 것보다 낫다).

**3초 주입 → 주입 정지 → 2초 bounded quiesce → `requestStop()` + `join()`.**
TSan이 보통 5~15배 느려지는 것을 감안해도 90초 TIMEOUT에 약 25배 여유. **어떤 assert도 처리율을 보지 않는다** —
전부 상한, 0, 또는 "증가했다(`> 0`)"다. 처리율 수치는 assert가 아니라 리포트로 간다 (CI 박스의 처리량은 계약이 아니다).

게이트별 assert:

| 게이트 | assert |
| --- | --- |
| Thread ownership | 인프로세스 assert 없음. TSan 클린 + Debug의 `assertOwnerThread()`. harness는 join 전에 `metrics()`를 절대 읽지 않고 `progress()`와 자기 atomic만 본다 |
| No Worker blocking | `active_stall_episodes == 0` (shutdown episode는 분리 — D4) **AND** 모든 active phase `max_cpu_residence < D4의 phase별 correctness threshold` **AND** 일반 Debug active phase `voluntary_context_switches == 0` **AND** active wall `max_residence < fairness_threshold` **AND** 일반 Debug `PollWait.max_residence < max_poll_timeout * 2`. Actor도 Actors phase의 CPU/context-switch로 같은 판정을 쓰고 `actor.turn_slice_ns` wall histogram은 관측용이다. 부하 실재 증거는 `operation_timeouts > 0` **AND** latency가 **band 안에** 있을 것: `operation_latency_ns.max >= operation_timeout * 0.8` (느린 DB가 실제로 있었다) **AND** `<= operation_timeout + timeout_overshoot_limit` (timeout이 터무니없이 늦게 처리되지 않았다). p50/p95/p99는 리포트용 |
| Memory bound | **1차 근거는 HWM이 아니라 admission/rejection 테스트다** (피드백 5). HWM은 "실제 부하에서 얼마까지 썼는가"라는 관측이고, "절대 cap을 못 넘는다"를 증명하는 것은 admission 테스트다. 증명 구조: (1) cap이 존재하고 (2) cap 직전까지 accepted, (3) cap 초과는 rejected, (4) accounting 불변식 유지, (5) load에서 exact/sampled HWM이 cap 이하. load test는 (5)만 담당하고 (1)~(4)는 `tests/worker_metrics_test.cpp`의 자원별 admission 케이스가 담당한다. load에서는 추가로 HWM 중 3개 이상이 `> 0`(부하가 실재했음)을 확인 |
| Single await state | **구조적 불변식으로 문서화한다**: `ActorSlot.blocked`가 `optional<variant>` **하나**라는 타입 구조 자체가 canonical owner의 증거이고, 여기에 기존 DB timeout/resume 테스트(late completion·stale timeout no-op)를 근거로 붙인다. `stale_completions == 0`은 이 게이트의 직접 증거가 아니므로 근거에서 뺀다 |
| Stale safety | load test에서는 **`network.invariant_violations == 0`만** 확인한다. stale 발생 여부는 타이밍 의존이라 `합 > 0`은 flaky하고, counter 증가는 "stale mutation 0"의 직접 증거도 아니다 (피드백 4). 진짜 증명은 **deterministic 주입 테스트**가 한다 — 틀린 generation/incarnation/operation ID를 의도적으로 주입하고 **대상 상태가 unchanged임을 assert**한다 (10G-stale, 아래) |
| Fairness | `entries > 0` **AND** 각 phase `max_entry_gap < fairness_threshold` (D4에서 유도, 기본 budget·Debug 기준 약 424ms). `entries > 0`만으로는 3초 동안 처음·마지막 한 번씩만 돌아도 통과한다 |
| Shutdown | `actor_phases_duration <= worker_shutdown_timeout`, `db_shutdown_duration <= db_shutdown_timeout`, 두 `*_deadline_exceeded == false`, phase A~D `deadline_hit == false`, `actor.forced_blocked_destructions == 0`, group 케이스 `joinOverruns().empty()`. **`phase_d` 잔여 자원은 아래 표대로 자원별로 판정한다** (피드백 부수 — 기록 필드는 풍부한데 판정에서 빠져 있었다) |

**잔여 자원 판정 — 측정 시점을 명시한다** (피드백 1). Actor/connection은 `phase_d.remaining`에서,
DB는 `_db->shutdown()` 반환 뒤의 `final_resources`에서 본다:

| snapshot | 자원 | 판정 | 근거 |
| --- | --- | --- | --- |
| `phase_d.remaining` | `connections` | **0 필수** | phase D가 남은 전부를 `forceClose`한다 |
| `phase_d.remaining` | `actors` | **0 필수** | phase D가 남은 전부를 `removeActor(ShutdownForced)`한다 |
| `phase_d.remaining` | `blocked_actors` | **0 필수** | 남으면 coroutine leak — Shutdown 게이트의 핵심 |
| `phase_d.remaining` | `loading` | **0 필수** | activation load는 phase B에서 `ActivationCancelled`로 취소된다 |
| `phase_d.remaining` | `inbox_bytes` | **0 필수** | phase D가 `_inbox.close()` 후 drain한다 |
| `phase_d.remaining` | `timer_entries` | **잔여 허용** | application timer는 phase B에서 취소되지만 내부 timer는 남을 수 있다 |
| `phase_d.remaining` | `application_timer_bytes` | **0 필수** | 취소가 accounting까지 되돌렸는지. snapshot에 필드가 있어야 판정 가능하므로 위 struct에 포함했다 |
| **`final_resources`** | `db_in_flight`, `db_queued` | **0 필수** | `phase_d`에서 보면 DB shutdown 이전 값이라 의미가 없다. 10E의 async deadline이 강제 정리를 보장한다 |
| Behavior parity | **저율 probe client**가 `Ping`마다 `Pong` 수신(피드백 6), `network.protocol_errors == 0`, `invariant_violations == 0`. 강한 parity 진술은 기존 `worker_network_integration_test.cpp` / `worker_adapter_test.cpp`를 무변경으로 재실행하는 쪽에 남긴다 |

**10G-slow — deterministic slow-consumer 테스트** (피드백 4). 테스트 하나가 너무 많은 것을 증명하려
하지 않도록 의미 증명과 종합 압력을 분리한다:

```text
worker_slow_consumer_deterministic_test  → EAGAIN / EPOLLOUT / watermark 의미 증명
worker_load_scenario_test                → I/O + Actor + DB + inbox 동시 압력에서
                                            boundedness / fairness / stall / shutdown 증명
```

결정적 테스트의 구조 — **기다리지 않고 강제한다**:

```text
작은 SO_SNDBUF(server측) + 작은 SO_RCVBUF(client측) + client가 recv() 안 함
  → 큰 response를 enqueue (bounded attempts 또는 bounded bytes 한도 안에서)
  → EAGAIN
  → EPOLLOUT wait
  → soft/hard watermark
  → 해당 연결만 close ([src/worker/worker.cpp:2703](../src/worker/worker.cpp#L2703))
```

- server측 `SO_SNDBUF` 하향은 이미 있는 `WorkerNetworkConfig::client_send_buffer_size`
  (`std::optional<int>`)로 가능하다 — 프로덕션 API 추가 0.
- `ConnectionLimits`의 soft/hard도 수십 KiB로 낮춘다 (이 테스트 자기 config에만).
- **`max_attempts` / `max_bytes`에 도달했는데 EAGAIN을 만들지 못하면 명시적으로 FAIL한다.**
  조용히 통과시키면 테스트가 아무것도 증명하지 않은 채 초록색이 된다.
- 다른 연결이 영향받지 않았음(`graceful_closes`/다른 연결의 정상 응답)도 함께 확인한다.

**10G-stale — deterministic stale 주입 테스트** (load test에서 뺀 것을 대체). `WorkerActorTestAccess`로
단일 Worker를 직접 구동하면서 (1) 죽은 incarnation 앞으로 온 `completeDb`, (2) 이미 완료된 operation의
late timeout, (3) 옛 generation의 connection action을 각각 주입하고, 주입 **전후로 대상 slot/connection
상태를 캡처해 unchanged임을 assert**한다. 해당 stale counter가 정확히 1 증가하는 것도 함께 본다.
기존 1·5단계 테스트가 이미 일부를 덮으므로 빠진 조합만 채운다.

**10G-metrics — deterministic 계측 검증.** [tests/worker_progress_test.cpp](../tests/worker_progress_test.cpp)와
[tests/worker_watchdog_test.cpp](../tests/worker_watchdog_test.cpp)는 다음 계약을 직접 증명한다.

- CPU spin은 active CPU residence를 증가시키고 voluntary switch는 0이다.
- `sleep`/명시적 blocking handler와 blocking Actor는 voluntary switch를 검출하며 CPU가 wall보다 작다.
- 정상 idle/load의 active phase voluntary switch는 0이다.
- 동일 CPU에 고정한 normal-priority competitor가 nice 19 Worker를 선점하면 wall residence와 involuntary
  switch는 커지지만 CPU residence는 correctness threshold 이하, voluntary switch는 0이다.
- watchdog은 같은 sleeping handler/Actor를 wall-clock episode로 검출한다.

**새 실행 바이너리는 만들지 않는다.** 종료 조건이 "신규 Worker runtime **test path**"이고 ctest가 그 경로다.
`src/server_main.cpp`는 여전히 legacy `GameServer`를 구동하며 그것을 바꾸는 것이 11단계의 항목이다.
지금 `snf_worker_server`를 만들면 11단계가 즉시 정리해야 할 두 번째 진입점이 생긴다.
대신 **리포트 헬퍼**를 만든다: 신규 `tests/worker_metrics_report.hpp`의
`printWorkerReport(std::ostream&, const WorkerMetrics&, const DbClientMetrics&)`
([src/server_main.cpp:87](../src/server_main.cpp#L87)의 `print_metrics`/`format_distribution` 형태).
load 테스트가 무조건 출력하므로 `ctest -R snf_worker_load` 출력이 리포트 측정값의 복붙 원본이 된다.

### 10H — 품질 게이트 리포트

ctest `LABELS` 추가: worker/db/layer 테스트 + 신규 load 2개에 `worker`, load 쌍에 `load`를 붙인다.
`SKIP_RETURN_CODE 77`은 실제 CMake 등록과 같은 **4개**다:
`snf_worker_load_mysql`, `snf_db_conformance_mysql`, `snf_worker_db_client_mysql`,
`snf_mysql_integration_tests`.

신규 `docs/worker-runtime-quality-gates.md` (한국어, `docs/room-load-measurement.md`의 톤):

```
# 10단계 Worker runtime 품질 게이트 리포트
> 문서 상태 / 측정 대상 / 이 수치가 무엇이 아닌지
## 1. 환경          image, kernel, arch, CPU·memory, compiler, 측정일
## 2. 재현 명령      preset별 docker 한 줄 명령 표
## 3. 부하 시나리오 설정  WorkerBudgets / WorkerNetworkConfig / WorkerActorConfig /
                        WorkerInboxConfig / DbClientConfig / WorkerWatchdogConfig 전 필드
## 4. threshold와 근거  실행 전 고정한 preset multiplier, phase별 single-item allowance,
                        공식. 조정했다면 calibration 실행과 근거를 분리해 기록
## 5. 게이트별 판정   게이트 | 명령 | 관측 지표 | 측정값 | 판정   (8행)
                        Memory bound는 admission/rejection(증명)과 HWM(관측)을 나눠 적는다
## 6. 측정값 원본     printWorkerReport 출력 그대로. exact HWM과 sampled HWM을 구분 표기
## 7. Watchdog 관측   active/shutdown stall episodes, longest_stall_ns, 임계값 근거
## 8. Shutdown 계측   phase A~D 표 (duration / deadline_hit / 잔여 자원) + post-DB final_resources
                        **MySQL final teardown boundedness: 8단계 conformance 근거 / 이번 라운드 미측정**
## 9. 한계와 후속     MySQL 경로 미측정 명시, 11단계에서 production 경로로 재실행할 항목
```

최종 gate 통과 뒤 [docs/development-roadmap.md:155](./development-roadmap.md#L155)의 체크박스 6개와
[docs/architecture/unified-worker-runtime.md:688](./architecture/unified-worker-runtime.md#L688)의 완료 상태,
[§14 표의 10단계 행](./architecture/unified-worker-runtime.md#L704)을 함께 갱신했다.

---

## 검증

각 스텝은 Docker 안에서만 빌드한다.

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace snf-server-dev bash -lc 'cmake --build --preset debug && ctest --preset debug --output-on-failure'
```

게이트별 재현 명령. **preset별 build를 반드시 함께 넣는다** — 깨끗한 환경에는 tsan/asan build가 없어
`ctest --preset tsan`만으로는 재현되지 않는다. 아래는 전부 docker 래퍼의 `bash -lc '...'` 안에 들어가는
내용이며, **표가 아니라 코드 블록에 둔다** (표 셀에서 `|`를 이스케이프하면 복붙이 깨진다).

Thread ownership — TSan과 owner assertion 두 갈래:

```bash
cmake --build --preset tsan && TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan -L worker
```

```bash
cmake --build --preset debug && ctest --preset debug -L worker
```

No Worker blocking / Fairness — 같은 load 실행에서 서로 다른 지표를 본다 (tsan에서도 반복):

```bash
cmake --build --preset debug && ctest --preset debug -R snf_worker_load_stub
```

Memory bound — admission/rejection(증명)은 `-L worker`, HWM(관측)은 load:

```bash
cmake --build --preset asan-ubsan && ctest --preset asan-ubsan -R snf_worker_load
```

Single await state / Stale safety — deterministic 테스트가 근거:

```bash
cmake --build --preset debug && ctest --preset debug -L worker
```

Shutdown:

```bash
cmake --build --preset tsan && ctest --preset tsan -R 'snf_worker_load|snf_worker_tests'
```

Behavior parity — 전체:

```bash
cmake --build --preset debug && ctest --preset debug
```

TCP integration:

```bash
cmake --build --preset debug && ctest --preset debug -R snf_worker_network_integration_tests
```

MySQL integration — **이번 라운드 미측정, 명령만 기록.** 환경변수는 `ctest` 앞에 놓아야 한다
(`cmake --build` 앞에 두면 build에만 적용되고 ctest에는 전달되지 않는다):

```bash
export SNF_MYSQL_TEST_HOST=... && cmake --build --preset debug && ctest --preset debug -L mysql
```

## 최종 실행 증거

calibration은 공식 gate 전에 끝냈고 threshold는 Debug `8`, ASan/UBSan `20`, TSan `40`과 phase별 allowance로
고정했다. 이후 공식 gate 중에는 값을 변경하지 않았다. 상세 설정·원본 수치·판정은
[Worker runtime 품질 게이트 리포트](./worker-runtime-quality-gates.md)에 기록했다.

- same-CPU scheduler 선점 실측: wall 130.059ms, 같은 sample CPU 10.012ms, voluntary 0,
  involuntary 5. wall-only 판정의 false positive와 CPU/context-switch 분리의 유효성을 확인했다.
- Debug load 반복: 5/5 PASS. Debug full: 17개 등록, 13 PASS, MySQL 4 SKIP, 실패 0.
- ASan/UBSan worker label: 11개 등록, 8 PASS, MySQL 3 SKIP, sanitizer 오류/leak 0.
- TSan worker label(`TSAN_OPTIONS=halt_on_error=1`): 11개 등록, 8 PASS, MySQL 3 SKIP, race 0.
- TCP integration, deterministic slow-consumer/stale/watchdog/shutdown, clang-format, `git diff --check`,
  worker-layer 검사를 통과했다.
- MySQL 환경은 제공되지 않아 미측정이며, CMake의 `SKIP_RETURN_CODE 77` 4개와 재현 명령을 기록했다.

합의된 MySQL 미측정 범위에서 8개 품질 게이트가 모두 PASS이므로 Stage 10 판정은 **GO**다.

루프를 건드리는 10B/10C/10E 직후에는 타이밍에 민감한 `worker_loop_test`/shutdown 케이스 때문에
`ctest --preset debug` 전체를 돌린다. `max_queued_bytes` 강제 직후에는
`snf_worker_db_client_stub`과 `snf_db_conformance_stub`을 즉시 확인한다.

## 리스크

- ~~**metrics struct 위치 기반 aggregate 초기화**~~ — 확인 완료. 4개 metrics 타입 어디에도 위치 기반
  brace-init이 없어(`include/`·`src/`·`tests/` 전체 grep) 필드 추가는 안전하다. 기존 assert는 전부
  `metrics().field` 형태라 기본 초기화로 커버된다.
- **`max_slice_duration` 의미를 바꾸지 않는다.** histogram은 나란히 추가한다.
- **watchdog 소멸 순서** — `_workers` 뒤에 선언 + 소멸자에서 명시적 stop.
- **watchdog 발화 테스트 flakiness** — 400ms vs 50ms로 8배 여유. edge-triggered라 `== 1`이 잘 정의되고,
  실제 실행에서 흔들리면 그때만 `>= 1`로 완화한다.
- **`max_queued_bytes` 강제는 동작 변경** — 기존에 통과하던 submit이 `Rejected`가 될 수 있다.
  기본값 1MiB에 기존 테스트는 `max_queued_operations = 2`라 op 캡이 먼저 걸리지만 확인이 필요하다.
- **load 설정의 낮은 watermark는 load 시나리오 전용이다.** `ConnectionLimits` 기본값을 바꾸는 것이 아니라
  harness가 자기 config에만 적용한다. 기본값을 건드리면 기존 통합 테스트의 slow-consumer 경계가 흔들린다.
- **`db_shutdown_timeout` 신설은 `DbClientConfig`의 `isValid()`와 기존 생성 지점 전부에 영향** —
  기본값이 있으므로 designated init은 안전하지만 validator 확장을 잊지 말 것.
- **`DbClient::shutdown()` 시그니처 변경(deadline 인자 추가)은 호출부와 기존 shutdown 테스트에 영향.**
  내부 drain 루프에 deadline을 전파하는 것이 10E에서 가장 침습적인 변경이므로 이 커밋 직후
  `snf_worker_db_client_stub`과 shutdown 관련 케이스를 우선 확인한다.
- **threshold(D4)는 최초값이 추정이다.** Debug/ASan/TSan 실측 후 조정하되, 조정한 값과 근거를
  리포트에 남긴다. 게이트를 통과시키려고 threshold를 올리는 것과 실측 기반으로 보정하는 것을 구분한다.

## 범위 밖 (11단계 또는 로드맵 §5)

- `src/server_main.cpp` 변경, 신규 서버 바이너리 (production 경로 전환은 11단계 항목)
- `snf::runtime::Distribution` 병합/삭제 (`snf_runtime`이 제거되는 11단계까지 블록)
- 로깅 라이브러리, metrics exporter, 주기적 리포터 thread, 대시보드 (계측·기록이 요구사항이고,
  프로덕션 하드닝은 이 프로젝트 범위 밖)
- watchdog 자동 대응 (auto-stop/restart/thread dump)
- worker 경로를 통한 Room/Zone/Battle 부하 (아직 서빙하지 않는 프로토콜 — 11단계의 게이트 재실행)
- actor live migration, dynamic worker scaling, idle passivation
- phase별 latency histogram, connection write-drain latency (D3에서 기각. 게이트가 실제로 요구하면 재검토)
