# Phase 7.3 Durable Ranking — 저장·재생 계약

## 1. 목적과 범위

Phase 7.2는 Player별 단조 sequence, global offset, 결정적 ranking projection과 checkpoint/tail replay의
의미를 in-memory reference로 검증했다. Phase 6.3은 Player snapshot과 구매 transaction을 MySQL에
durable하게 저장한다. 그러나 둘을 그대로 조합하면 score snapshot 저장과 event publish가 다른 경계에
있어 다음 crash window가 남는다.

```text
PlayerActor score 변경
→ in-memory event publish
→ 나중 disconnect에서 Player snapshot 저장
```

event publish 뒤 snapshot 저장 전에 crash하면 event와 score가 모두 메모리에서 사라진다. snapshot
저장 뒤 event publish 전에 crash할 수 있는 구조로 순서를 바꾸면 score만 남고 event가 사라진다.
Phase 7.3의 목적은 Player score/sequence와 outbox event를 **하나의 durable transaction**으로 만들고,
projection이 durable checkpoint 뒤의 outbox tail만으로 항상 복구되게 하는 것이다.

이 단계는 다음을 포함한다.

- 신뢰된 score award의 idempotency identity
- Player score·sequence 갱신과 outbox append의 MySQL transaction
- strict global offset 순서의 durable event log
- 단일 projector의 ordered tail 적용과 durable checkpoint
- crash, duplicate, reverse completion과 shutdown 복구 테스트

client가 자기 점수를 올리는 wire command, 시즌 보상 지급, 여러 프로세스 projector leader election,
장기 archive storage는 포함하지 않는다.

## 2. 소유권과 authoritative state

- gameplay 규칙은 `AwardRankingScoreCommand`를 만들지만 Player score의 authoritative write는
  `PlayerRepository` transaction 하나다.
- `PlayerActor`는 transaction completion의 절대 score와 sequence만 적용한다. DB 요청 전에 score를
  미리 변경하지 않는다.
- MySQL durable mode에서 `snf_player_events`가 ranking event의 authoritative log다. in-memory log는
  테스트 reference일 뿐이며 durable mode의 source of truth가 아니다.
- `RankingProjection` mutable state는 projector 하나만 변경한다. Actor Worker, DB Worker와 network
  reactor는 projection map을 직접 수정하지 않는다.
- DB Worker와 projector는 Actor, `ActorSlot`, coroutine handle이나 runtime mutable state를 보유하지
  않는다. repository completion은 기존 owning-Worker continuation 경계를 사용한다.

## 3. Score award identity

각 trusted award는 non-zero 64-bit `award_id`를 가진다. identity scope는 `(PlayerId, award_id)`다.
`request_id`는 process-local correlation이므로 durable identity로 사용하지 않는다.

```cpp
struct RankingAwardRequest {
    PlayerId player;
    RankingAwardId award_id;
    std::uint64_t score_delta;
};
```

- 같은 identity와 같은 `score_delta`는 기존 결과를 replay한다.
- 같은 identity를 다른 delta로 재사용하면 `IdempotencyConflict`다.
- `score_delta == 0`, score overflow와 sequence overflow는 transaction 전에 명시적 거부다.
- retry 결과는 최초 commit이 만든 event score, Player sequence와 global offset을 그대로 반환하고,
  별도로 현재 authoritative Player score/sequence를 반환한다. Actor는 후자만 적용하므로 오래된 award
  replay가 이후 award의 상태를 되돌리지 않는다.
- award producer는 성공 completion을 받지 못하면 같은 identity로 재전달할 수 있다. 새 identity를
  만들면 별도 award로 처리되는 것이 의도된 의미다.

## 4. MySQL transaction

schema version 2는 다음 durable outbox state를 추가한다.

```text
snf_event_stream(stream='ranking', last_offset)
snf_player_events(global_offset PK,
                  player_id,
                  player_sequence,
                  award_id,
                  score_delta,
                  absolute_score,
                  created_at,
                  UNIQUE(player_id, player_sequence),
                  UNIQUE(player_id, award_id))
```

`AUTO_INCREMENT`는 rollback된 insert의 값을 재사용하지 않아 strict offset에 gap을 만들 수 있으므로
사용하지 않는다. transaction이 `snf_event_stream`의 ranking row를 `FOR UPDATE`로 잠그고
`last_offset + 1`을 할당한다. rollback은 cursor 갱신도 되돌리므로 committed log의 offset은 1부터
연속이다. 이 row는 award transaction을 직렬화하지만 우선 correctness reference로 채택한다. 측정이
병목을 증명하면 partitioned stream이나 projection의 gap 허용 계약을 별도 단계에서 비교한다.

신규 award transaction 순서는 다음과 같다.

```text
START TRANSACTION
→ Player row INSERT IGNORE
→ Player row SELECT ... FOR UPDATE
→ (PlayerId, award_id) 조회
  ├─ 같은 delta: 저장된 event replay 후 COMMIT
  └─ 다른 delta: conflict 후 COMMIT
→ score/sequence overflow 검사
→ ranking stream cursor SELECT ... FOR UPDATE
→ Player score와 last_domain_event_sequence UPDATE
→ outbox event INSERT
→ stream cursor UPDATE
→ COMMIT
```

Player update, event insert와 cursor advance 중 하나라도 실패하면 전부 rollback한다. commit 뒤 Actor
completion이 유실돼도 outbox tail과 같은 identity retry가 결과를 복구한다. repository는 SQL 오류를
내부에서 무제한 재시도하지 않고 `Unavailable`로 반환하며 connection을 폐기한다.

## 5. Actor 적용과 runtime 순서

`AwardRankingScoreCommand`는 `award_id`와 delta를 가진다. persistent Player binding은 구매와 같은
별도 stage에서 repository operation을 await한다.

- completion이 `Committed` 또는 `Replayed`면 PlayerActor가 현재 authoritative score와 sequence를
  적용한다. outbox 원본의 event score/sequence는 projection과 감사에 사용하고 Actor snapshot 적용에는
  사용하지 않는다.
- `Unavailable`과 conflict는 Actor state를 바꾸지 않는다.
- completion의 Player, award identity와 delta가 pending command와 다르면 runtime invariant 위반이다.
- repository transaction 자체가 event를 durable하게 만들었으므로 Actor completion 순서대로
  projection을 직접 갱신하지 않는다. 서로 다른 DB Worker의 offset 2 completion이 offset 1보다 먼저
  도착할 수 있기 때문이다.
- 이 internal command는 client response가 없어 outbound slot을 소비하지 않는다. terminal accounting은
  repository completion 뒤 Actor state 적용 시 끝난다.

in-memory repository도 같은 request/result API와 idempotency 의미를 구현해 Actor·binding 테스트가
backend에 의존하지 않게 한다.

## 6. Ordered projector와 checkpoint

projector는 하나의 전용 thread가 소유하며 blocking MySQL read/checkpoint write를 Actor Worker에서
분리한다. 시작 시 listener를 열기 전에 다음 순서로 복구한다.

```text
durable checkpoint 읽기
→ RankingProjection restore
→ checkpoint_offset 이후 outbox를 offset 오름차순으로 batch replay
→ tail 도달
→ gameplay ingress 시작
```

실행 중에는 commit notification 또는 짧은 bounded polling interval로 tail을 읽는다. notification은
hint일 뿐이며 유실돼도 polling이 복구한다. query는 `global_offset > current_offset ORDER BY
global_offset LIMIT batch_size`이고, 첫 record가 `current_offset + 1`이 아니면 corruption/failure로
승격한다.

checkpoint는 generation snapshot과 meta pointer를 사용한다.

```text
snf_ranking_checkpoint_meta(singleton_id, global_offset, generation)
snf_ranking_checkpoint_entries(generation, player_id, score, player_sequence,
                               PK(generation, player_id))
```

schema version 3의 작은 reference 구현은 stream cursor를 잠근 transaction 안에서 전체 standings를
교체했다. 7.3c 측정에서 이 구간이 award max를 키워 schema version 4로 전환했다. v4는 다음 generation
entry를 먼저 쓴 뒤 stream cursor와 meta row를 잠그고 pointer만 atomic swap한다. snapshot row 작성은
award cursor를 보유하지 않는다. commit 뒤 이전 generation을 회수하며, commit 직후 crash로 두 generation이
남아도 다음 save 시작 시 현재 generation보다 오래된 row를 정리한다. checkpoint 실패는 live projection을
되돌리지 않으며 failure metric을 올린 뒤 durable tail을 유지한다.

outbox row는 checkpoint가 성공했다고 지우지 않는다. 현재 identity 범위에는 season이 없고 outbox의
`(PlayerId, award_id)` unique row가 replay 증거도 겸한다. 따라서 시간 기반 prune은 오래된 award retry를
새 award로 오인하게 만든다. 현 single-season schema는 outbox 전체 보존을 정책으로 택한다. 향후 prune은
season identity와 별도 receipt tombstone, final checkpoint, 검증된 archive/backup이 모두 갖춰지고 모든
projector가 확인한 offset 이하만 대상으로 한다.

## 7. Shutdown과 failure

정상 종료 순서는 기존 runtime lifecycle에 다음 predicate를 추가한다.

```text
gameplay ingress close
→ Actor/repository transaction drain
→ committed ranking tail 확인
→ projector가 목표 offset까지 catch up 시도
→ optional checkpoint
→ projector stop
```

grace deadline이 먼저 오면 projector catch-up을 포기해도 committed outbox는 남는다. 다음 시작이
checkpoint tail을 재생한다. 따라서 projection drain은 Player transaction commit을 rollback하거나
서버 종료를 무기한 막는 조건이 아니다.

다음은 서로 다른 failure다.

- repository queue full: award 미적용, 명시적 `Unavailable`
- transaction SQL error: rollback, connection 폐기, `Unavailable`
- commit 뒤 completion 유실: Player/outbox는 적용됨, 같은 award retry는 replay
- projector read 실패: gameplay write는 계속 가능하지만 projection lag/failure metric 증가
- projection 순서·내용 conflict: silent skip 금지, projector failure
- checkpoint 실패: outbox를 보존하고 다음 checkpoint에서 재시도

운영 readiness 정책에서 projection lag이 허용치를 넘으면 ranking read를 stale 표시하거나 차단할 수
있지만 Player gameplay write를 자동 rollback하지 않는다.

## 8. 상한과 관측

- repository queue와 Worker 수는 Phase 6.3 상한을 공유한다.
- projector batch size, 회차당 최대 batch 수와 polling interval로 live poll의 DB 작업량을 제한한다.
- outbox는 process-memory queue가 아니며 임의 eviction을 허용하지 않는다. 현재 single-season 전체 보존은
  구조적으로 unbounded하므로 DB row/byte capacity를 운영에서 감시하고 season schema 전에는 prune하지
  않는다.
- 최소 metric은 award commit/replay/conflict/unavailable, repository latency, projector applied/duplicate/
  rejected, projection lag(`committed_tail - applied_offset`), poll/checkpoint failure와 checkpoint latency다.

## 9. 완료 기준과 구현 순서

### 7.3a Durable award/outbox transaction (완료)

- request/result API와 in-memory reference를 추가한다.
- MySQL schema version 2와 atomic Player/outbox transaction을 구현한다.
- duplicate/conflict, 두 repository instance 경합, commit 직전/직후 process crash를 검증한다.
- Actor가 DB completion 전 score를 바꾸지 않고 authoritative completion만 적용하는지 검증한다.

구현 결과 MySQL schema version 2와 in-memory reference가 같은 identity/replay 의미를 제공한다. 실제
MySQL에서 동일 award 경합, strict offset, commit 직전 rollback과 commit 직후 replay를 Debug 반복,
ASan/UBSan 및 TSan으로 검증했다.

### 7.3b Projector recovery와 checkpoint (완료)

- ordered MySQL tail reader, startup replay와 live projector를 구현한다.
- checkpoint transaction과 restart restore를 구현한다.
- completion 역순, projector 중단/재시작, checkpoint write crash와 tail replay를 검증한다.
- projector lag/failure metric과 graceful shutdown catch-up을 server snapshot에 연결한다.

구현 결과 `RankingStore`가 in-memory와 MySQL adapter의 ordered tail/checkpoint 경계를 통일하고, 전용
projector thread가 construction에서 checkpoint restore와 tail catch-up을 끝낸 뒤 live polling을
시작한다. MySQL schema version 3은 checkpoint meta/entry table을 추가했다. `GameServer`는 Actor runtime
drain 뒤 projector를 stop해 final catch-up/checkpoint를 시도하므로 `run()` 반환 뒤 standings와 metric이
committed tail을 반영한다. read와 checkpoint 실패는 live projection을 폐기하지 않고 각각 metric을
올린 뒤 재시도한다.

in-memory 테스트는 batch replay, live poll, read/checkpoint 실패 재시도와 stop 직후 final drain을
검증한다. 실제 MySQL 테스트는 checkpoint restore 뒤 tail, live award, restart, checkpoint commit
직전 rollback과 commit 직후 process crash, `GameServer` 재시작 복구를 검증한다. Debug 실제 MySQL
경로 5회 반복과 ASan/UBSan·TSan 전체/실제 MySQL 경로가 통과했다.

### 7.3c 운영 부하와 retention 결정 (완료)

- score award rate와 Player 수를 올려 stream cursor lock, DB queue와 checkpoint latency를 측정한다.
- cursor가 병목일 때만 stream partition/gap 계약을 재검토한다.
- season/backup 요구를 기준으로 outbox archive와 idempotency retention 창을 결정한다.

Release/MySQL 8.0.46 기준선에서 1/2/4 Worker 처리량은 `990/1,149/1,092 awards/s`였고 Worker 수를
늘릴수록 award p99가 `1.704/3.670/5.767 ms`로 증가했다. strict stream cursor가 병목임은 확인됐지만
현재 콘텐츠의 요구율보다 충분한 기준선이므로 gap/partition으로 의미를 복잡하게 만들지 않는다.

5,000 Player full snapshot에서 v3 checkpoint p99 `156.429 ms`와 award max `123.479 ms`를 확인해 schema
v4 generation pointer swap을 도입했다. v4 checkpoint 총비용 p99는 `182.917 ms`지만 snapshot 작성 중
stream lock을 보유하지 않아 award max가 `71.542 ms`로 줄었다. live poll도 회차당 batch 수를 bounded해
지속 입력 중 checkpoint가 밀리지 않는다. retention은 위 §6의 single-season 전체 보존으로 결정했다.
재현 명령과 전체 수치는 [ranking performance baseline](./ranking-performance-baseline.md)에 있다.

7.3 뒤의 콘텐츠 순서는 cross-zone handoff다. 이전 destination drain, 새 destination activation과 route
공개를 하나의 protocol로 검증할 실제 gameplay 요구가 생긴 뒤 구현한다. 이동 coalescing, Zone 분할,
ConnectionScope와 UnifiedRuntime은 각각 측정 임계점을 넘을 때만 승격한다.
