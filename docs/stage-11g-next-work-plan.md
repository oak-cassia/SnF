# 11G 후속 작업 계획 — 실제 workflow 실패 경로 대조

> 상태: 다음 작업 계획. 11G 전체 완료를 의미하지 않는다.
> 기준: 11G-3B2의 Sink pending 보존·재시도 검증 이후.
> 최종 실행 기준은 [stage-11 계획](./stage-11-workflow-migration-plan.md)이다.

## 다음 작업: 11G-4A — 실패 주입과 계약 대조표

제품 코드 수정에 앞서 이 세션에서 실제 Worker + Player/Zone/Room adapter의 경계를 검증한다.
11G-3B2 신규 테스트의 관찰용 Actor는 transport admission과 retry를 검증한다. 11G-1의 실제 도메인
cleanup 테스트와 연결되는 통합 증거는 아래에서 보강한다.

### 범위와 산출물

- 수정 대상: `tests/worker_adapter_test.cpp`, 필요 시 `tests/worker_session_test.cpp`,
  stage-11 계획과 Room/cross-zone 계약의 검증 대조표.
- Worker, Sink, Player, Zone, Room 제품 코드는 이 단계에서 수정하지 않는다.
- 대조표 열: 계약 조항 / 기존 상태·단계 / 실패 주입 지점 / client terminal / route·location /
  실제 Zone participant·Room 좌석 / pending·timer / 기존 또는 신규 테스트 / 남은 차이.
- legacy topology 설명과 Worker의 승인된 의미 변경을 분리한다. 테스트를 통과시키기 위해 계약을
  약화하거나, 알려진 ghost participant를 정상 완료 조건으로 삼지 않는다.

### 수행 순서

1. **B2 → 실제 cleanup 통합**
   - 로컬 및 2-Worker에서 인증·Zone 입장 후 disconnect를 발생시킨다.
   - Player mailbox 포화, 통지 inbox 포화, receipt inbox 포화/유실을 각각 주입한다.
   - 용량 회복 후 정상 Worker loop의 retry로 pending이 0이 되고, Player binding 해제와 실제 Zone
     participant 0을 확인한다. receipt 도착 시점과 cleanup 실행 시점을 별도로 관찰한다.
   - Entering/Returning/Transferring 중 명령 적용 후 outcome 유실 시점도 포함한다.
   - 이전 generation의 close/receipt/outcome을 재접속 전후에 전달해 새 세션과 새 점유가 보존되는지 확인한다.
   - thread 간 관측은 atomic mirror 또는 테스트 동기화를 사용한다. 실행 중 owner 전용 map과 metric을
     다른 thread에서 직접 읽지 않는다.
2. **cleanup tell 실패 재현**
   - Player가 close를 처리한 뒤 발행한 Zone Leave 또는 Room Leave 자체를 mailbox/inbox 포화로 거절한다.
   - `effect_tell_failures` 또는 remote delivery failure와 실제 남은 점유를 대조한다.
   - B2 pending이 이미 해제될 수 있음을 검증한다. mailbox receipt를 domain cleanup 완료로 오인하지 않는다.
   - 현재 best-effort 정책의 한계를 보고한다. 이 단계에서 범용 reliable tell이나 재시도 queue를 추가하지 않는다.
3. **failure terminal과 stale 검증 대조**
   - Room Join 두 단계, Return, cross-zone 세 단계에서 timer admission 거절 / 명령 거절 /
     명시적 실패 / 적용 후 outcome 유실 / stale identity / 중복 outcome을 대조한다.
   - Room Join/Return outcome의 request·connection generation·zone·epoch 검증을 우선 확인한다.
   - 하나의 client command에 성공/실패 응답이 중복되지 않고, close 결정 이후 queued 입력이 새로운
     점유를 만들지 않는지 확인한다. cleanup 성공 여부는 별도 열에 기록한다.
4. **shutdown 차이 목록**
   - stop 직전 pending close, Entering/Returning/Transferring 중 shutdown을 재현한다.
   - 현재 phase B는 application timer를 취소하고 blocked Actor를 종결하지만 Player workflow는
     `ActorSlot.blocked`가 아니다. 이 둘을 같은 cancel로 취급하지 않는다.
   - grace 내 drain과 deadline 강제 제거를 구분하고, pending·workflow cancel·최종 persistence의
     미보장 항목을 기록한다. 이 단계에서 shutdown barrier나 DB 정책을 변경하지 않는다.

### 완료 기준

- 위 경계에 대한 재현 결과와 계약 조항별 대조표가 있고, 각 항목이 검증 완료/재현된 격차/미검증으로 구분된다.
- 회귀 테스트는 기존 Debug 전체, TSan worker, ASan/UBSan worker, TCP 포함 adapter 5회 반복,
  변경 줄 clang-format 및 `git diff --check`로 검증한다.
- 아직 해결하지 않은 결함의 기대 동작은 실패 주입 재현으로 보고하며, 의도적으로 실패하는 테스트를
  기본 suite에 커밋하지 않는다. 제품 수정과 함께 통과할 회귀 테스트로 승격한다.
- 이 단계가 끝나도 11G 체크박스를 닫지 않는다.

## 이후 분리할 작업

| 순서 | 작업 | 진입 조건 / 역할 |
| --- | --- | --- |
| 11G-4B | 대조표에서 재현된 Player terminal·stale 검증 결함의 최소 수정 | 이 세션에서 기대 동작·허용 함수 확정 → 다른 세션에 제품 코드만 위임 → 이 세션에서 회귀 검증·커밋 |
| 11G-4C | cleanup 전달 실패 계약과 구현 | cleanup owner·용량 사전 확보·완료 기준·세대/epoch fence·종결 불가 정책을 먼저 합의 |
| 11G-5 | shutdown workflow cancel과 최종 persistence | admission 차단 → 승인 작업 drain → 명시적 cancel → resource 해제의 순서 및 absolute deadline 정책을 먼저 확정 |
| 11G 마감 | 전체 failure terminal 대조표 및 완료 게이트 | 미보장 항목을 완료로 표기하지 않고 11I 과부하 실측 항목과 구분 |
| 11H | production Worker 경로 전환 | 11G 완료 후 시작 |

cleanup 재시도는 특히 Room의 새 입장을 이전 Leave가 제거하지 않도록 세대/epoch 식별이 필요할 수 있다.
단순히 B2의 receipt를 재사용하거나 일반 `TellActorEffect`를 무기한 재전송하는 설계를 선행 승인 없이 적용하지 않는다.
하위 모델에는 조사·설계 결정을 섞어 맡기지 않고, 재현 및 계약 결정 이후 파일·함수·금지 사항이 고정된
제품 코드 프롬프트를 별도로 작성한다.
