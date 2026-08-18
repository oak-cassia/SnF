# Actor 주도 Timer Scheduling 결정

> 상태: 채택  
> 범위: Zone/BattleRoom 등 Actor의 tick·timeout 예약과 Runtime의 timer 책임

## 1. 결정

Actor가 반복 여부, 간격과 다음 실행 시점을 포함한 **시간 정책**을 소유한다. ActorRuntime은 특정 Actor의
mailbox에 command를 미래 시점에 한 번 전달하는 **one-shot scheduling mechanism**만 제공한다.

```text
Actor command 처리
→ 상태 전이
→ 필요하면 다음 TimerRequest 반환
→ Binding이 Worker-local timer에 one-shot 예약
→ 만료 시 같은 Actor mailbox에 command 게시
```

Runtime은 Zone이나 주기 실행을 알지 못한다. Runtime이 보관하는 것은 pending one-shot의 deadline,
Actor identity와 command뿐이다. 별도의 `ZoneTimerService`와 `Arm`/`Cancel` 기반 periodic lifecycle은
두지 않는다.

## 2. 소유권과 동작 계약

- Actor는 자신의 상태에 따라 다음 timer를 요청하거나 요청하지 않는다.
- 반복 실행은 timer가 자동 반복되는 것이 아니라 각 command 결과가 다음 one-shot을 다시 요청해 구성한다.
- 실행 중지와 주기 변경은 각각 다음 timer를 반환하지 않거나 다른 delay를 반환하는 상태 전이로 표현한다.
- Binding은 Actor의 `TimerRequest`를 Runtime scheduling primitive로 변환할 뿐 도메인 정책을 판정하지 않는다.
- Runtime은 예약 시 mailbox capacity를 확보하고, 만료된 command를 owning Worker에서 직접 게시한다.
- 이미 예약된 one-shot이 상태 전이 뒤 한 번 도착할 수 있다. Actor는 현재 상태를 확인해 이를 무해하게
  처리하며, cancel을 correctness의 필수 조건으로 삼지 않는다.
- Actor eviction과 Worker shutdown에서는 Runtime이 pending timer와 그 reservation을 함께 폐기한다.
- 다음 timer가 없고 mailbox·task·operation·continuation이 모두 비면 Actor는 passivation할 수 있다.

예를 들어 실행 중인 Zone은 tick 결과로 다음 tick을 요청하고, 빈 Zone은 요청하지 않는다.

```text
Running + Tick → 상태 갱신 + 다음 tick 요청
Running + Tick → Cleared + timer 없음
Waiting         → timer 없음
ReadyCheck      → timeout one-shot 요청
```

## 3. Timing semantics

현재 `TimerRequest{delay}`는 handler가 결과를 반환한 시점부터 delay 뒤에 실행하는 **fixed-delay**다.
따라서 처리 시간과 Worker 지연만큼 주기가 늦어질 수 있으며 정확한 fixed-rate 또는 20 Hz 실행을
보장하지 않는다.

fixed-rate가 필요한 도메인은 periodic timer를 Runtime에 추가하지 않는다. Actor가 이전 logical deadline과
interval을 상태로 소유하고 다음 deadline을 계산해 one-shot을 요청한다. 놓친 구간을 보정하거나 건너뛰는
정책 역시 Actor가 결정한다.

## 4. 결과와 trade-off

이 결정으로 Actor 상태와 timer 상태를 별도 서비스 사이에서 동기화할 필요가 없어진다. `Arm`/`Cancel`,
pending cancellation과 cancellation race가 줄고, 상태별 tick·timeout 정책, passivation과 shutdown 순서가
단순해진다. 같은 Runtime primitive를 Zone tick, battle tick, ready timeout, respawn 등에 재사용할 수 있다.

대신 timer 감지와 command 실행이 Worker 부하에 영향을 받는다. hot Actor는 같은 Worker의 timer lateness를
늘릴 수 있고, 반복마다 timer heap pop/push 비용이 발생한다. 이 비용을 이유로 별도 timer service를 미리
도입하지 않고 부하 측정 결과로 판단한다.

## 5. 관측과 검증

- timer lateness의 p50/p95/p99/max
- scheduling 수, capacity 거부, 발화, 취소와 stale discard 수
- tick 실행 시간과 budget overrun
- fixed-delay drift와, fixed-rate를 도입한 Actor의 skipped interval
- 마지막 participant 이탈과 timer 만료의 경합에서 추가 예약이 생기지 않는지
- passivation·eviction·shutdown 뒤 timer와 outstanding reservation이 남지 않는지
- timer heap 규모 증가가 Worker 처리량과 지연에 미치는 영향

별도 TimerService 도입은 timer 정확도를 Worker 부하에서 분리해야 한다는 측정 결과가 있고, 그 이점이
추가 상태 소유권과 lifecycle 복잡도를 정당화할 때만 다시 검토한다.
