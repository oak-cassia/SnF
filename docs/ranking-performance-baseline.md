# Phase 7.3c Ranking 부하·retention 기준선

## 범위와 환경

2026-08-13 local Docker의 Release build와 MySQL `8.0.46`에서 trusted score award, strict stream cursor,
projector poll과 full-snapshot checkpoint를 함께 측정했다. 이 수치는 구현 비교 기준이며 배포 환경의
capacity SLO가 아니다. 각 case는 빈 전용 database에서 실행했고 score delta는 모두 1, 실패·replay는
0이었다.

재현 도구는 `snf_ranking_benchmark`다. 다음 값들을 함께 출력한다.

- committed awards/s와 submit-to-completion 지연
- repository award transaction 지연, queue high-water와 실패
- projector poll/checkpoint 지연
- committed tail, applied/checkpoint offset과 최종 lag

```bash
SNF_MYSQL_HOST=127.0.0.1 \
SNF_MYSQL_USER=snf \
SNF_MYSQL_PASSWORD=secret \
SNF_MYSQL_DATABASE=snf_ranking_bench \
./build/release/snf_ranking_benchmark \
  --players 100 --awards 2000 --workers 2 \
  --queue-capacity 1024 --max-in-flight 512 \
  --checkpoint-every 1000 --poll-ms 20
```

## Stream cursor와 Worker 수

100 Player에게 2,000 awards를 보내고 checkpoint 주기는 1,000으로 고정했다. 높은 in-flight 값은 DB를
의도적으로 포화시키기 위한 것이므로 end-to-end queue 지연은 gameplay SLO로 해석하지 않는다.

| DB Worker | 처리량 (awards/s) | award p99 | award max | queue high-water | checkpoint p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 989.97 | 1.704 ms | 11.176 ms | 512 | 15.134 ms |
| 2 | 1,149.12 | 3.670 ms | 23.156 ms | 511 | 15.283 ms |
| 4 | 1,091.98 | 5.767 ms | 71.553 ms | 509 | 17.013 ms |

2 Worker에서 1 Worker 대비 처리량은 약 16% 늘지만 4 Worker는 더 늘지 않고 transaction p99/max만
악화된다. 모든 award가 strict global cursor 한 row를 잠그므로 이 결과는 예상한 직렬화 지점과
일치한다. 현재 기본값 2 Worker를 유지한다. 배포 환경의 지속 목표가 그 환경에서 다시 잰 포화 처리량의
50%를 넘거나 award p99 예산을 위반할 때만 partitioned stream 또는 gap 허용 projection을 재검토한다.

## Poll 상한과 checkpoint 규모

첫 측정에서는 live projector가 tail이 완전히 빌 때까지 `catchUpAll()`을 계속해 지속 입력 중
checkpoint 주기 1,000을 지키지 못하고 종료 시점까지 write를 미뤘다. live poll을 회차당
`batch_size × max_batches_per_poll`로 제한하고 batch 사이 checkpoint를 시도하도록 고쳤다. 이후 5,000
events case에서 checkpoint write 5회와 tail/applied/checkpoint `5000/5000/5000`, lag 0을 확인했다.

| Player / awards | Worker | 처리량 | award p99 | award max | checkpoint p99 | writes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 / 5,000 | 4 | 1,114.19/s | 6.291 ms | 72.733 ms | 45.041 ms | 5 |
| 5,000 / 5,000, schema v3 | 4 | 1,036.36/s | 6.291 ms | 123.479 ms | 156.429 ms | 5 |
| 5,000 / 5,000, schema v4 | 4 | 1,071.36/s | 6.291 ms | 71.542 ms | 182.917 ms | 5 |

v3는 전체 checkpoint entry 교체 동안 stream cursor를 잠갔다. v4는 새 generation row를 먼저 쓰고
마지막 meta pointer swap만 stream cursor와 원자화한다. 전체 checkpoint 시간 자체는 generation 작성과
이전 generation 회수 때문에 줄지 않았지만 award max와 총 처리량은 개선됐다. fault-injection 통합
테스트는 snapshot row 작성 직후 pointer swap을 멈춘 동안 별도 award transaction이 완료됨을 검증한다.
정상 save 뒤 entry table에는 활성 generation 하나만 남는다.

schema v3→v4 checkpoint 보존, pointer swap 전 award 진행, commit 직전·직후 crash와 restart 경로는 실제
MySQL Debug 5회 반복 및 ASan/UBSan·TSan 전체 테스트를 통과했다.

## Retention 결정

현재 `snf_player_events`는 projection source인 동시에 `(PlayerId, award_id)` replay 증거다. row를 시간으로
지우면 같은 award가 늦게 retry될 때 새 score로 다시 적용될 수 있다. 현재 schema에는 season identity도
없으므로 자동 TTL/prune은 도입하지 않는다.

운영 정책은 다음과 같다.

- 현 single-season stream과 award receipt는 전체 보존한다.
- row count와 table/index bytes를 관측해 storage planning 입력으로 사용하되, 용량 압박으로 임의 삭제하지
  않는다.
- season rotation을 구현할 때 `SeasonId`를 award identity와 stream에 넣고, final checkpoint와 archive를
  검증하며, prune 뒤에도 retry를 판별할 receipt tombstone 보존 창을 함께 정한다.
- 이 조건이 갖춰진 뒤에만 모든 projector가 확인한 final offset 이하의 이전 season outbox를 삭제한다.
