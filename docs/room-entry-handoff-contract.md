# Room 입장 Handoff 계약

> 범위: 한 프로세스 안에서 Player를 `ZoneActor`에서 `RoomActor`로 옮기고, 전투가 끝나면 되돌리는
> 상태 전이
>
> 구성요소별 역할과 tell·completion·saga 선택 기준은
> [Actor 상호작용 아키텍처](./actor-interaction-architecture.md)에 있다.

## 1. 해결하는 문제

Room을 Zone 위의 overlay로 두면 전투 중 Player가 두 공간에 동시에 존재한다. 그러면 "전투 중 Move는
되는가", "AOI에는 보이는가", "위치는 Zone과 Room 중 누가 소유하는가"가 전부 정책 질문이 되고, 그
정책은 Zone과 Room 양쪽에 흩어진다.

이 계약은 전투 중 Player가 Zone에 없다고 정한다. 위 질문들은 답이 아니라 대상이 사라진다.

보장하는 것:

- 전투 중 Player는 정확히 하나의 authoritative 공간에 속한다.
- 단계별 실패에 명시적 보상이 있고, 입장 경로에는 Zone 복구가 없다.
- client command 하나에 terminal outcome 하나다.
- 복귀는 server가 시작하며 unsolicited 프레임 하나로 끝난다.
- transition 기록에 고정 상한이 있다.

matchmaking, Party 단위 동시 입장, 프로세스 간 이동, Room 상태 영속화와 mid-battle reconnect는 범위가
아니다.

## 2. 소유권과 상태

reactor의 `RouteCoordinator`가 Player의 현재 공간을 소유한다. `RoomActor`는 좌석과 phase를,
`PlayerActor`는 전투 스냅샷의 출처인 경험치를 소유한다.

```text
Stable(zone, epoch, position)
Transferring(zone -> zone)                  기존 cross-zone handoff
Entering(room, source route, request_id)    신규
InRoom(room, return zone, return position)  신규
Returning(room -> zone, return_epoch)       신규
```

- connection마다 진행 중 transition은 최대 하나다. zone handoff와 room entry는 서로를 배제한다.
- reactor는 좌석을 세지 않는다. `max_participants`와 phase는 Room만 알고 있으므로 입장 판정은 Room
  command 하나로 끝나며, 별도의 예약 command와 예약 상태는 두지 않는다.
- 전투 스냅샷은 `PlayerActor`가 만든다. 경험치를 소유한 곳에서 읽는다는 규칙은 그대로다.
- `PlayerRecord`의 `last_location`은 **복귀 지점**으로 유지한다. Room 재적은 영속화하지 않는다.
  저장된 위치는 재시작 뒤 복원될 수 있어야 하고, Room은 재시작을 넘기지 못한다.

## 3. 정상 입장

```text
RoomJoin 프레임: Stable(zone)과 인증된 Player 확인
→ Entering 기록과 completion slot 예약
→ PlayerActor JoinRoomRequest: 전투 스냅샷 생성
→ RoomActor JoinRoom(스냅샷): 좌석과 phase 판정
→ Applied 확인
→ source Zone LeaveZone(source_epoch): authoritative position 확보
→ Left 확인
→ InRoom 공개, zone route 폐기
→ RoomJoined 응답과 client command terminal release
```

Room 판정이 Zone leave보다 먼저다. 거절 사유인 `RoomFull`, `WrongPhase`, `AlreadyJoined`는 모두 Room만
알 수 있고, Room을 나중에 물으면 거절될 때마다 더 큰 epoch으로 source Zone을 복구해야 한다. 그것이
cross-zone handoff에서 가장 비싼 경로다. 이 순서에서는 입장 경로에 Zone 복구가 존재하지 않는다.

대가는 `JoinRoom` 적용과 Zone `Left` 사이의 짧은 구간이다. 이 구간에서 Player는 두 공간에 기록돼
있지만, client command는 `TransitionInProgress`로 끝나고 `StartBattle`도 InRoom을 요구하므로 전투는
시작될 수 없다. 관측되는 결과는 남의 AOI에 잠시 남는 것뿐이며, 전투 시간이 아니라 mailbox 왕복
두 번으로 상한이 잡힌다.

## 4. 정상 복귀

복귀는 client 요청이 아니다.

```text
BattleCleared(boss 사망) | BattleFailed(Room 자기 deadline) | LeaveRoom | Disconnected
→ Returning 기록과 return_epoch 발급
→ return Zone EnterZone(return_epoch, return position)
→ Applied 또는 AlreadyPresent 확인
→ Stable(zone) 공개, InRoom 폐기
→ ReturnedToZone(request_id = 0)
```

- `return_epoch`은 Player별 단조 증가 epoch에서 새로 발급한다. 복귀 Enter는 전투 전에 남아 있던 어떤
  stale zone command보다 뒤여야 한다.
- return Zone은 그동안 passivate됐을 수 있다. `ActivateIfMissing`으로 다시 활성화되고, Zone은 순수
  in-memory 상태 기계이므로 빈 Zone의 첫 참가자로 들어간다. 이는 정상 동작이다.
- clear는 요청이 없으므로 release token도 없다. `BattleCleared`가 이미 unsolicited인 것과 같은 이유다.
- 복귀는 보상이 아니라 종결에 달려 있다. 실패는 아무에게도 지급하지 않으므로, 복귀 요청은
  `RoomResult::grants`가 아니라 `audience`(Room이 아직 들고 있는 참가자 전원)를 읽는다. 그래야
  `Cleared`와 `Failed`가 같은 경로로 모두를 원래 Zone에 돌려놓는다.

## 5. 실패와 보상

| 실패 지점 | 보상 |
| --- | --- |
| Entering admission: 상한 또는 진행 중 transition | 아무것도 바꾸지 않고 거절 |
| PlayerActor 게시 실패 | Entering 폐기, `Stable(zone)` 유지 |
| Room mailbox가 join tell을 거부 | Player의 binding이 `EntryFailed` completion을 게시한다. 기다리는 completion이 오지 않는 유일한 조용한 경로이고, saga에는 timeout이 없다 |
| Room 거절: Full / WrongPhase / AlreadyJoined | Entering 폐기, `Stable(zone)` 유지, 거절을 그대로 응답 |
| Zone leave 게시 또는 적용 실패 | `LeaveRoom` 보상 게시, `Stable(zone)` 유지, `EntryFailed` 응답 |
| 응답 큐 포화 | connection 종료. 이미 Room에 있으므로 disconnect 경로가 `LeaveRoom`을 낸다 |
| 복귀 Enter 실패 | stable route를 추측하지 않는다. session location을 known none으로 확정하고 connection을 닫는다 |

`EntryFailed`는 `RoomCommandStatus` 끝에 추가한다. status는 바이트로 와이어에 나가므로 기존 값의
자리를 바꾸지 않는다.

## 6. Disconnect와 reconnect

- `Entering` 중 disconnect: 진행 중 단계를 먼저 끝내고 보상한다. `JoinRoom`이 이미 적용됐으면
  `LeaveRoom`을 낸다.
- `InRoom` 중 disconnect: `LeaveRoom`으로 Room에서 제거한다. 전투 보상은 포기하며, clear는 그때 남아
  있는 참가자에게만 지급된다.
- 따라서 reconnect는 Room을 모른다. 저장된 위치가 복귀 Zone이므로 `EnterZone`이 평소처럼 복원한다.
- mid-battle reconnect를 지원하려면 Room 재적을 어딘가에 남겨야 하고, 그 순간 Room 상태의 수명이
  process 수명을 넘는다. 이 슬라이스의 비범위인 이유다.

## 7. 프레임

- `RoomJoin`과 `RoomJoined`의 왕복은 그대로다. 응답 시점만 Zone leave 뒤로 옮겨진다.
- `ReturnedToZone`을 추가한다. unsolicited이며 복귀한 Zone과 좌표를 담는다.
- `InRoom`에서 도착한 `Move`, `LeaveZone`, `EnterZone`은 `ZoneCommandStatus::InRoom`으로 답한다.
  `InvalidPayload`는 connection을 닫으므로 쓰지 않는다. 입장이 완료되는 순간 이미 날아온 `Move`는
  프로토콜 오류가 아니라 평범한 client race다. 입장·복귀가 진행 중일 때는 `TransitionInProgress`로
  답한다 — 그때는 실제로 전이 중이다.
- `StartBattle`은 보낸 connection이 해당 Room에 `InRoom`일 것을 요구한다.

## 8. 검증 조건

- 정상 입장 뒤 Player는 Room에만 있고, source Zone의 participant에서 사라진다.
- Room 거절은 source Zone을 건드리지 않는다. 거절 뒤 Move가 여전히 원래 epoch으로 적용된다.
- Zone leave 실패는 `LeaveRoom` 보상을 내고 좌석을 되돌린다.
- clear 뒤 복귀 Enter가 새 epoch으로 적용되고, 복귀 전에 zone route가 노출되지 않는다.
- passivate된 return Zone으로의 복귀가 성공한다.
- InRoom 중 disconnect는 좌석을 해제하고 그 Player 없이 clear가 지급된다.
- Entering 중 disconnect가 좌석, transition 기록, token, reservation을 남기지 않는다.
- 전투 중 Move/LeaveZone/EnterZone이 Zone 상태를 바꾸지 않는다.
- shutdown이 진행 중 Entering과 Returning을 명시적으로 cancel한다.
- 실제 TCP에서 zone enter → room join → battle start → clear → zone 복귀가 왕복한다.
