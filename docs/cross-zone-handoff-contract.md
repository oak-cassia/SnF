# Cross-Zone Handoff 계약

> 문서 상태: **gameplay 전이 의미 보존 / Worker target 구현 완료 (11F)**
> epoch, stale 방어와 실패 보상은 보존한다. `reactor`, `RouteCoordinator`와
> `ZoneTransitionChannel`은 legacy parity oracle의 이름이다. target runtime은 `PlayerActor`의 explicit
> workflow state와 Actor-to-Actor mailbox 응답으로 구현했으며, 상세 결과는
> [11단계 실행 계획의 11F 결과](./stage-11-workflow-migration-plan.md#11f-결과)를 따른다.
>
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

legacy에서는 reactor의 `RouteCoordinator`가 route와 transition을 소유했다. target runtime에서는
`PlayerActor`가 route와 transition을 소유하며, `ZoneActor`는 계속 자신의 participant와 위치만 수정한다.
Zone 결과는 immutable `ZoneOutcomeMessage`로 PlayerActor mailbox에 회신되고 Worker는 route나 session
객체를 직접 참조하지 않는다.

```text
Stable(zone, epoch, position)
Transferring(correlation_id,
             source_zone, source_epoch,
             target_zone, target_epoch,
             requested_position,
             step, request_id)
```

- connection마다 handoff는 최대 하나다.
- correlation ID와 route epoch은 0이 아닌 단조 증가 값이다.
- `Transferring` 동안 gameplay와 두 번째 Enter는 `TransitionInProgress`로 끝낸다.
- completion은 connection generation, handoff ID, step과 epoch이 모두 일치할 때만 적용한다.

## 3. 정상 전환

```text
Stable source와 인증 Player 확인
→ LeaveSource timeout admission
→ route를 Transferring(LeaveSource)으로 변경
→ source Leave(source_epoch)
→ LeaveSource Applied 확인
→ EnterTarget timeout admission
→ target Enter(target_epoch, requested_position)
→ EnterTarget Applied/AlreadyPresent 확인
→ Stable(target, target_epoch, authoritative position) 공개
→ session location 갱신
→ ZoneEntered 응답과 client command terminal release
```

전환 시작 전에 승인된 source command는 source mailbox FIFO에서 먼저 끝난다. 전환 시작 뒤 command는
source에 게시하지 않고, target route는 target activation이 확인되기 전까지 공개하지 않는다.

## 4. Backpressure와 client outcome

legacy handoff admission은 completion slot 하나를 전체 수명 동안 예약했다. target Worker 구현은
`LeaveSource`, `EnterTarget`, `RestoreSource` 각 단계의 내부 Zone command를 발행하기 전에
`TimerAdmission::tryReserve`로 독립된 1초 application timer를 예약하고, 전체 handoff에 같은 correlation과
서로 다른 step을 사용한다. 다음 단계 timer admission이 실패하면 새 command를 발행하지 않고 known-none
cleanup과 close로 끝낸다.

등록된 timer를 조기 취소하는 API가 없으므로 완료된 이전 단계의 one-shot timer가 만료 때까지 잠시 겹칠 수
있다. correlation과 현재 step이 모두 일치하는 timer만 terminal을 실행하고 이전 step timer는 stale no-op이다.
따라서 한 handoff의 timer reservation은 최대 3개로 고정되며, 내부 Zone command는 여전히 한 번에 하나만
in-flight다. Actor mailbox의 outcome tell 적용 실패는 발신 actor가 동기 관측할 수 없지만, 각 단계 timeout이
이를 terminal cleanup과 close로 바꾼다.

내부 Leave/Enter/cleanup command는 client credit을 만들지 않는다. transition record 하나가 최초
`EnterZone`의 request와 `CommandReleaseToken`을 소유한다.

- 성공: `ZoneEntered` 하나를 enqueue하고 release
- 복구 가능한 실패: `TransferFailed` 하나를 enqueue하고 release
- send queue 포화: connection을 닫고 release
- stale completion: 현재 token을 변경하지 않음

## 5. 실패와 보상

### Source 변경 전

LeaveSource timeout admission이 실패하면 source를 수정하지 않고 기존 `Stable(source)`를 유지한 채 failure를
응답한다. target Worker의 `TellActorEffect` 적용은 actor turn이 끝난 뒤 Worker에서 일어나므로 source command
post 실패를 PlayerActor가 동기 관측할 수 없다. 이 경우 LeaveSource timeout에서 적용 여부 불명으로 취급해
양쪽 cleanup과 close로 끝낸다.

Source Leave가 `StaleRoute`를 반환하면 outcome에 실린 실제 source participant epoch으로 cleanup한다.
요청의 source epoch을 재사용하지 않으며, route는 known-none으로 확정하고 connection을 닫는다.

### Source leave 뒤

target의 **확정된 적용 실패 outcome**은 더 큰 `restore_epoch`으로 source Enter를 게시한다. restore가 성공한
뒤에만 source route를 다시 공개하고 failure를 응답한다. 단, `StaleRoute`는 target에 더 높은 epoch의
participant가 존재한다는 authoritative 결과이므로 일반 실패처럼 source를 복구하지 않는다. 결과의 epoch으로
target을 cleanup하고 known-none close로 끝낸다. target tell post 실패와 outcome 유실은 PlayerActor 관점에서
구분되지 않으므로 EnterTarget timeout의 적용 여부 불명 경로를 따른다.

EnterTarget 또는 RestoreSource timeout admission이 실패하면 이전 단계에서 source가 이미 변경됐으므로 해당
command를 발행하지 않고 양쪽 cleanup과 known-none close로 끝낸다.

target 적용 여부를 알 수 없거나 restore도 실패하면 stable route를 추측하지 않는다. source와 target에
epoch별 cleanup Leave를 게시하고 connection을 닫으며 session location을 `known none`으로 만든다.
RestoreSource의 `StaleRoute`도 실제 source participant epoch으로 cleanup한다. 세 단계 모두 outcome의 identity를
검증한 뒤 관측 epoch을 반영하며, `_route_epoch`은 기존 값과 관측값 중 큰 값을 유지해 재접속 뒤 입장 epoch이
관측값보다 커지도록 한다.

target Enter 적용 뒤 route 공개 전에 disconnect되면 target cleanup을 먼저 끝낸다. target을 잠시 stable로
공개해 새 입력을 받지 않는다.

connection close를 결정한 순간부터 같은 generation의 새 client input을 차단한다. owner Worker가 close를
적용하고 `PlayerConnectionClosedMessage`를 돌려주기 전에 mailbox에 대기하던 요청도 새 transition을 시작하지
못한다.

## 6. Disconnect와 shutdown

**11G-1에서 확인한 Worker 구현 범위:** generation이 일치하는 `PlayerConnectionClosedMessage`가 도착하면
transfer 상태를 값으로 복사해 기존 known-none helper를 호출한다. source와 target에 epoch별 Leave를 발행하고
binding/pending/workflow를 해제하며, location은 nullopt로 만든다. restore 단계의 source cleanup은 restore
epoch을 사용한다. 추가 restore Enter, client 응답, close effect나 workflow timer는 생성하지 않는다.
실제 양쪽 Zone adapter로 각 단계의 disconnect 점유 제거 및 stale event no-op을 검증했다.

다음 흐름은 전체 계약의 목표이며, 11G-1은 **통지 및 cleanup tell 정상 전달 시**의 점유 제거만 검증했다.
11G-2는 application timeout의 일시적 mailbox 포화 재시도, 11G-3B2는 정상 loop의 close 통지/receipt
유실에 대한 bounded Sink 재시도를 검증했다. 이들은 실행 시간 상한이나 cleanup 완료 확인이 아니다.
실제 workflow 통합 과부하 검증, cleanup 성공 확인, final snapshot과 shutdown cancel은 아직 완료하지 않았다.

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
- target `StaleRoute`는 결과의 higher epoch participant를 cleanup하고 source와 target 양쪽에 남지 않는다.
- source Leave/Restore의 `StaleRoute`는 실제 participant epoch으로 cleanup하고 재접속 후 더 큰 epoch으로 입장한다.
- stale completion은 현재 transition을 진행시키지 않는다.
- close 결정과 `PlayerConnectionClosedMessage` 사이에 대기 중이던 Enter는 새 participant를 만들지 않는다.
- disconnect와 shutdown 뒤 중복 Entity, stale location, timer, token과 reservation이 남지 않는다.
- 최소 completion capacity와 churn에서도 설정 상한을 넘지 않는다.
- 실제 TCP에서 Zone A enter/move → Zone B enter/move/leave가 새 epoch으로 왕복한다.
