# Phase 7.4 Cross-Zone Handoff 계약

## 1. 목적과 범위

현재 `RouteCoordinator`는 연결마다 안정된 Zone route 하나를 소유한다. 같은 Zone 재입장은 멱등하지만
다른 Zone으로의 `EnterZone`은 거부한다. 단순히 source `Leave`와 target `Enter`를 연달아 enqueue하면
다음 실패 창이 생긴다.

```text
source Leave 적용
→ target Enter enqueue 실패 또는 결과 유실
→ route는 source를 가리키지만 Player는 어느 Zone에도 없음
```

반대로 target route를 먼저 공개하면 source mailbox에 이미 승인된 이동과 target 이동이 동시에 Player를
수정할 수 있다. `route_epoch`은 stale command를 검출하지만 이 전환 자체를 원자화하지 않는다.

Phase 7.4는 한 프로세스 안의 두 `ZoneActor` 사이에서 다음을 보장한다.

- source 입력 차단과 FIFO drain 뒤 leave
- target activation 완료 뒤에만 새 route 공개
- 각 단계의 mailbox 포화, stale result, disconnect와 shutdown에서 명시적 보상 또는 연결 종료
- client command credit과 wire outcome의 정확히 한 번 종결
- transition/completion 수와 메모리의 명시적 상한

프로세스 간 Zone 이관, DB-backed saga, seamless map streaming, 좌표 변환 규칙과 shard migration은 포함하지
않는다. target 좌표는 현재 `EnterZone` payload의 좌표를 그대로 사용한다.

## 2. 소유권과 실행 경계

전환 상태의 authoritative owner는 reactor thread의 `RouteCoordinator`다. Actor Worker는 source/target
Zone 상태만 변경하고 route map이나 session journal을 직접 수정하지 않는다.

Worker가 완료를 알릴 때는 immutable value만 bounded `ZoneTransitionChannel`에 게시한다.

```cpp
struct ZoneHandoffId { std::uint64_t value; };

enum class ZoneHandoffStep { LeaveSource, EnterTarget, RestoreSource, CleanupTarget };

struct ZoneHandoffCompletion {
    ZoneHandoffId handoff_id;
    ConnectionId connection;
    PlayerId player;
    ZoneId zone;
    std::uint64_t route_epoch;
    ZoneHandoffStep step;
    ZoneCommandStatus status;
    std::optional<ZonePosition> position;
};
```

completion에는 Actor, slot, coroutine handle, route/session의 pointer나 mutable reference를 넣지 않는다.
reactor는 connection generation, handoff identity, 현재 단계와 epoch을 모두 다시 검사해 stale completion을
폐기한다.

## 3. Route 상태와 identity

연결별 route는 다음 상태 중 하나다.

```text
Stable(zone, epoch, position)
Transferring(handoff_id,
             source_zone, source_epoch,
             target_zone, target_epoch,
             requested_position,
             step, request_id)
```

- 한 연결에는 handoff가 최대 하나다.
- `handoff_id`와 route epoch은 0을 쓰지 않는 단조 증가 값이며 overflow는 새 전환을 거부한다.
- target epoch은 source epoch보다 크다. source 보상이 필요하면 target epoch보다도 큰 restore epoch을 새로
  발급한다.
- `routeForGameplay()`는 `Stable`만 반환한다. `Transferring` 동안 Move, Leave와 두 번째 Enter는 Actor
  mailbox에 게시하지 않고 typed `TransitionInProgress` 결과로 끝낸다.
- source route는 전환 중 외부에 공개되지 않지만 보상 정보로 record 안에 남는다.
- `routeCountFor(zone)`은 timer 회수 판단에서 stable route와 해당 Zone을 아직 점유할 수 있는 transition
  단계를 함께 센다.

## 4. Completion channel 상한

`ZoneTransitionChannel`은 MPSC value queue와 reactor wake-up을 소유한다. 용량은
`max_zone_handoffs`와 같고 lifecycle capacity 이하로 설정한다.

한 handoff에는 동시에 내부 Zone command 하나만 in-flight다. handoff admission이 completion slot 하나를
수명 전체에 예약하므로 Worker의 단계 완료 publish는 queue 포화로 실패하지 않는다. reactor가 completion을
소비한 뒤 같은 reservation으로 다음 단계 하나를 게시한다. 다음 command를 게시할 수 없으면 §7의 보상을
수행한다.

회차당 처리하는 completion 수는 `max_zone_handoff_completions_per_turn`으로 제한한다. wake-up은 기존
reactor event와 공유할 수 있지만 queue, 예약 회계와 drain predicate는 outbound action과 분리한다.

## 5. 정상 전환 순서

다른 Zone으로 `EnterZone(target, requested_position)`이 오면 reactor는 다음 순서를 수행한다.

```text
1. Stable source와 인증 Player 확인
2. target timer 확보, handoff/completion slot과 target epoch 예약
3. route를 Transferring(LeaveSource)으로 변경
4. source mailbox에 internal Leave(source_epoch) 게시
5. LeaveSource Applied completion 확인
6. target mailbox에 internal Enter(target_epoch, requested_position) 게시
7. EnterTarget Applied 또는 AlreadyPresent completion 확인
8. Stable(target, target_epoch, authoritative target position) 공개
9. session location journal 갱신
10. source route 수가 0이면 source timer cancel
11. ZoneEntered 응답 enqueue와 client command terminal release
```

source `Leave`가 FIFO mailbox에서 실행되므로 3번 전에 승인된 source command는 먼저 끝난다. 3번 뒤의
gameplay command는 source에 게시되지 않는다. target route는 8번 전에는 조회되지 않으므로 target
activation보다 새 입력이 앞설 수 없다.

`PlayerMissing`, `StaleRoute` 또는 completion identity 불일치는 정상 성공으로 완화하지 않는다. route와
Actor 상태가 어긋난 invariant failure이며 §7로 간다.

## 6. Client credit과 wire outcome

handoff를 시작한 `EnterZone`은 source/target 내부 command 두 개가 아니라 client command 하나다.

- reactor transition record가 원래 connection/request와 `CommandReleaseToken` 하나를 소유한다.
- source leave, target enter와 보상 command는 reply 없는 internal submission이며 client credit을 만들지
  않는다.
- 성공 시 `ZoneEntered` 하나를 enqueue한 뒤 token을 release한다.
- 복구 가능한 실패는 `ZoneEntered` message type에 `TransferFailed` status와 복구된 stable route를 담아
  한 번 응답하고 release한다.
- 결과를 보낼 send queue 공간이 없으면 기존 per-session overflow 정책으로 연결을 닫고 token을
  release한다. response를 조용히 버리지 않는다.
- stale completion은 token을 건드리지 않는다.

`ProtocolGateway`는 이를 위해 lifecycle sink를 주입받지만 Worker-owned submission의 token을 빼내거나
공유하지 않는다. transition record와 token은 reactor에서만 생성·이동·파괴한다.

## 7. 실패와 보상

### 7.1 Source leave 전 실패

target timer, handoff slot 또는 source command admission이 실패하면 source Actor는 바뀌지 않았다.
transition을 `Stable(source, source_epoch)`으로 되돌리고 typed failure를 응답한다. 이 경우 source epoch을
증가시키지 않는다.

### 7.2 Source leave 뒤 target 실패

target command admission 실패 또는 명시적 non-applied result면 source에
`Enter(source, restore_epoch, last_stable_source_position)`을 게시한다.

- restore 성공 뒤에만 source route를 새 epoch으로 다시 공개하고 failure를 응답한다.
- restore command가 Full/Closed이거나 결과가 모순되면 stable route를 추측하지 않는다.
- target Enter가 적용됐는지 알 수 없는 completion 유실/채널 invariant failure에서는 source와 target에
  epoch별 best-effort cleanup leave를 게시하고 연결을 닫는다.
- 치명 실패에서는 session location을 `known none`으로 바꾼 뒤 Player close/save를 진행해 stale source
  위치가 영속 snapshot으로 남지 않게 한다.

target Actor가 `Applied`를 반환한 뒤 route publish 전에 연결이 닫히면 target leave를 먼저 drain하고
location을 none으로 확정한다. target을 새 stable route로 잠깐 공개해 새 client input을 받지는 않는다.

## 8. Disconnect와 shutdown

disconnect가 `Stable`에서 오면 현재 leave/save 순서를 유지한다. `Transferring`에서 오면 transition
record가 close continuation을 소유한다.

```text
새 client input 차단
→ 현재 단계 completion 또는 명시적 cleanup
→ source/target 점유가 없음을 확정하거나 fatal cleanup 기록
→ authoritative location 또는 known none 확정
→ Player ConnectionClosed/save 게시
```

따라서 Player close snapshot이 transition 중간의 source location을 섣불리 저장하지 않는다. 같은
connection generation의 handoff completion만 close continuation을 진행시킬 수 있다.

shutdown은 새 handoff admission을 먼저 닫는다. 이미 승인된 handoff completion channel은 Actor runtime과
함께 drain한다. grace가 먼저 끝나면 wire response는 포기할 수 있지만 route/token/completion reservation은
명시적으로 cancel하고 source/target cleanup 뒤 known-none snapshot 또는 다음 login 복구가 가능한 상태로
끝낸다. `LogicRuntimeDrained`만 보고 transition channel과 close continuation을 남긴 채 network를 종료하지
않는다.

## 9. Timer와 location journal

- target timer는 source leave 전에 확보한다. 실패하면 source stable 상태에서 전환을 거부할 수 있다.
- target timer가 이번 admission에서 생성됐고 target activation 전에 실패하면 cancel한다.
- source timer는 target stable publish 또는 source/target cleanup이 끝나고 source route count가 0일 때만
  cancel한다.
- session location은 target enter completion 전에는 갱신하지 않는다.
- 성공 시 Actor가 반환한 target position을 journal한다. 보상 성공 시 source restore 결과를 journal한다.
- fatal cleanup은 `known none`을 journal해 reconnect가 존재하지 않는 Entity 위치를 복원하지 않게 한다.

## 10. 관측과 완료 기준

최소 metric은 다음과 같다.

- handoff started/succeeded/failed/compensated/fatal
- 현재 pending과 high-water, admission rejected
- 단계별 mailbox wait와 transition duration p50/p95/p99/max
- stale completion과 identity mismatch
- disconnect 중 cleanup, shutdown cancel과 completion queue depth

자동화 테스트는 다음을 검증한다.

- source Move가 leave보다 먼저 FIFO 적용되고 transition 시작 뒤 Move는 source에 게시되지 않는다.
- target Enter completion 전에는 새 route와 target Move가 공개되지 않는다.
- 정상 전환은 Entity가 정확히 한 Zone에 있고 client terminal/response가 한 번이다.
- source post Full은 source stable을 유지한다.
- target post/result 실패는 더 큰 epoch으로 source를 복구한다.
- stale source/target completion은 현재 transition을 진행시키지 않는다.
- target 적용 직전·직후 disconnect가 source/target 중복 Entity나 stale saved location을 남기지 않는다.
- completion capacity 최소 구성과 churn에서 예약 수가 `max_zone_handoffs`를 넘지 않는다.
- shutdown drain/cancel 뒤 transition, token과 timer가 남지 않는다.
- 실제 TCP에서 Zone A enter/move → Zone B enter → moved/leave가 새 epoch과 좌표로 왕복한다.

## 11. 구현 순서

### 7.4a Transition state와 bounded completion (완료)

- `RouteCoordinator` stable/transferring 상태와 handoff identity를 추가한다.
- reserved `ZoneTransitionChannel`과 reactor turn/drain 계측을 추가한다.
- 내부 Zone command에 handoff step identity를 싣고 Worker result를 value completion으로 돌려준다.

구현 결과 `RouteCoordinator`가 connection별 stable/transferring 상태와 handoff identity, source/target
수명 회계를 소유한다. `ZoneTransitionChannel`은 handoff admission에서 고정 용량 ticket을 예약하고,
하나의 in-flight 단계가 그 slot을 재사용한다. 다른 identity와 중복 completion은 상태를 진행시키지 않으며,
queued completion과 release가 경합해도 consume 전에는 예약을 반환하지 않는다. Worker binding과 reactor
drain을 실제 source/target command에 연결하는 작업은 7.4b에서 수행한다.

### 7.4b Happy path와 protocol

- source leave → target enter → route publish 순서를 연결한다.
- transition 중 typed busy/failure와 client terminal ownership을 구현한다.
- timer/location journal과 TCP happy-path를 검증한다.

### 7.4c Compensation, disconnect와 shutdown

- target failure의 source restore, ambiguous result fatal cleanup을 구현한다.
- transition-aware `ConnectionClosed` continuation과 shutdown drain/cancel을 runtime predicate에 연결한다.
- 최소 capacity, stale completion, failure injection과 sanitizer 반복을 통과한다.
