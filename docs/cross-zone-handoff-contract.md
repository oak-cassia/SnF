# Cross-Zone Handoff 계약

> 범위: 한 프로세스 안의 두 `ZoneActor` 사이에서 Player를 옮기는 상태 전이

## 1. 해결하는 문제

source `Leave`와 target `Enter`를 독립 command로 보내면 중간 실패에 Player가 두 Zone에 존재하거나 어느
Zone에도 없는 상태가 될 수 있다. `route_epoch`은 stale command를 거르지만 전환 전체를 원자화하지는
않는다.

이 계약은 다음을 보장한다.

- source FIFO drain 뒤 leave
- target enter 완료 뒤 새 route 공개
- 단계별 실패의 명시적 보상 또는 cleanup
- client command 하나에 terminal outcome 하나
- transition과 completion 메모리의 고정 상한

프로세스 간 migration, DB-backed saga, map streaming과 좌표 변환은 범위가 아니다.

## 2. 소유권과 상태

reactor의 `RouteCoordinator`가 route와 transition을 소유한다. `ZoneActor`는 자신의 participant와 위치만
수정한다. Worker는 immutable completion value를 bounded `ZoneTransitionChannel`에 게시하며 route나
session 객체를 직접 참조하지 않는다.

```text
Stable(zone, epoch, position)
Transferring(handoff_id,
             source_zone, source_epoch,
             target_zone, target_epoch,
             requested_position,
             step, request_id)
```

- connection마다 handoff는 최대 하나다.
- `handoff_id`와 route epoch은 0이 아닌 단조 증가 값이다.
- `Transferring` 동안 gameplay와 두 번째 Enter는 `TransitionInProgress`로 끝낸다.
- completion은 connection generation, handoff ID, step과 epoch이 모두 일치할 때만 적용한다.
- timer 회수 시 stable route뿐 아니라 transition이 점유할 수 있는 Zone도 센다.

## 3. 정상 전환

```text
Stable source와 인증 Player 확인
→ target timer, handoff와 completion slot 예약
→ route를 Transferring(LeaveSource)으로 변경
→ source Leave(source_epoch)
→ LeaveSource Applied 확인
→ target Enter(target_epoch, requested_position)
→ EnterTarget Applied/AlreadyPresent 확인
→ Stable(target, target_epoch, authoritative position) 공개
→ session location 갱신
→ ZoneEntered 응답과 client command terminal release
```

전환 시작 전에 승인된 source command는 source mailbox FIFO에서 먼저 끝난다. 전환 시작 뒤 command는
source에 게시하지 않고, target route는 target activation이 확인되기 전까지 공개하지 않는다.

## 4. Backpressure와 client outcome

handoff admission이 completion slot 하나를 전체 수명 동안 예약한다. 한 handoff에는 내부 Zone command
하나만 in-flight이며, 단계가 바뀌어도 같은 reservation을 재사용한다. 따라서 승인된 Worker completion이
queue 포화로 유실되지 않는다.

내부 Leave/Enter/cleanup command는 client credit을 만들지 않는다. transition record 하나가 최초
`EnterZone`의 request와 `CommandReleaseToken`을 소유한다.

- 성공: `ZoneEntered` 하나를 enqueue하고 release
- 복구 가능한 실패: `TransferFailed` 하나를 enqueue하고 release
- send queue 포화: connection을 닫고 release
- stale completion: 현재 token을 변경하지 않음

## 5. 실패와 보상

### Source 변경 전

timer, handoff slot 또는 source command admission이 실패하면 source를 수정하지 않고 기존
`Stable(source)`를 유지한 채 failure를 응답한다.

### Source leave 뒤

target admission 또는 적용이 실패하면 더 큰 `restore_epoch`으로 source Enter를 게시한다. restore가
성공한 뒤에만 source route를 다시 공개하고 failure를 응답한다.

target 적용 여부를 알 수 없거나 restore도 실패하면 stable route를 추측하지 않는다. source와 target에
epoch별 cleanup Leave를 게시하고 connection을 닫으며 session location을 `known none`으로 만든다.

target Enter 적용 뒤 route 공개 전에 disconnect되면 target cleanup을 먼저 끝낸다. target을 잠시 stable로
공개해 새 입력을 받지 않는다.

## 6. Disconnect와 shutdown

```text
새 client input 차단
→ 진행 중 completion 또는 cleanup 처리
→ source/target 점유 제거 확인
→ authoritative location 또는 known none 확정
→ Player ConnectionClosed와 final snapshot
```

shutdown은 새 handoff admission부터 닫고 승인된 completion을 Actor Runtime과 함께 drain한다. grace가
끝나더라도 route, token과 reservation은 명시적으로 cancel한다. active transition이 남아 있으면 network를
drained로 판정하지 않는다.

## 7. 검증 조건

- source Move가 leave보다 먼저 적용되고 전환 뒤 Move는 source에 게시되지 않는다.
- target completion 전에 target route가 노출되지 않는다.
- 정상 전환 뒤 Player는 정확히 한 Zone에 있고 response와 terminal은 한 번이다.
- source post 실패는 기존 stable route를 유지한다.
- target 실패는 더 큰 epoch으로 source를 복구한다.
- stale completion은 현재 transition을 진행시키지 않는다.
- disconnect와 shutdown 뒤 중복 Entity, stale location, timer, token과 reservation이 남지 않는다.
- 최소 completion capacity와 churn에서도 설정 상한을 넘지 않는다.
- 실제 TCP에서 Zone A enter/move → Zone B enter/move/leave가 새 epoch으로 왕복한다.
