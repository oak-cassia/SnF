# SnF 개발 로드맵

> 상세 설계는 [서버 아키텍처 초안](./server-architecture-draft.md)을 참조한다.

## 목표

SnF를 `epoll` 네트워크 서버에서 다음 구조를 가진 게임 서버로 발전시킨다.

```text
Network Runtime
→ Sharded Actor Runtime
→ Coroutine Actor
→ Live Content
→ Minimal World/Battle
```

실행 순서는 기존 계획의 `1 → 2 → 3 → 4 → 8 → 6 → 7`이며, 아래에서는 실제 진행
순서대로 다시 번호를 매겼다.

## 1. Network와 GameRuntime 분리

```text
epoll Reactor
→ InboundCommand
→ GameWorker
→ OutboundMessage
→ eventfd
→ Reactor send
```

- 논리 `ConnectionId + generation`을 도입한다.
- inbound/outbound bounded queue를 구현한다.
- 현재 PING/PONG을 GameWorker까지 왕복시킨다.
- 순서, stale response, queue 포화와 graceful shutdown을 검증한다.

완료 기준: 기존 단위·통합·1,000 연결 부하 테스트가 새 경계를 거쳐 통과한다.

## 2. Sharded ActorRuntime

- `actor_id`를 기준으로 담당 Worker를 결정한다.
- 동일 Actor는 항상 한 번에 한 Worker에서만 실행한다.
- Worker별 bounded queue와 queue 지연 metric을 추가한다.
- 최소 두 Worker에서 서로 다른 Actor가 병렬 실행되는지 검증한다.

완료 기준: 동일 Actor의 순서와 단일 실행, 서로 다른 shard의 병렬 실행이 테스트된다.

## 3. PlayerActor와 Mailbox

- PlayerActor가 유저 영구 상태를 소유한다.
- 일반 Command는 Actor mailbox에 순서대로 적재한다.
- `Idle`, `Ready`, `Running`, `Suspended` 상태를 정의한다.
- ready queue 중복 등록과 특정 Actor의 Worker 독점을 방지한다.

완료 기준: 한 PlayerActor의 Command가 유실·중복·동시 실행 없이 처리된다.

## 4. Coroutine Suspend와 Resume

- Actor handler가 실제 비동기 대기에서만 `co_await`하도록 한다.
- suspend 중 같은 Actor의 일반 Command는 mailbox에서 기다리게 한다.
- Worker는 suspend된 Actor 대신 다른 Actor를 처리한다.
- 완료 continuation은 원래 ActorRuntime으로 돌아와 coroutine을 재개한다.
- blocking DB 드라이버는 bounded DB Worker Pool 뒤에서 실행한다.

완료 기준: 한 Actor가 I/O를 기다리는 동안 다른 Actor가 진행되고, 기존 coroutine이 먼저
완료된 뒤 mailbox 처리가 재개된다.

## 5. io_uring Network Backend

- 게임 계층이 `epoll` 구현에 의존하지 않도록 `NetworkRuntime` 경계를 유지한다.
- `IoUringNetworkRuntime`을 별도 completion 기반 backend로 추가한다.
- operation과 buffer lifetime, cancel, stale completion을 처리한다.
- ActorRuntime에는 동일한 `InboundCommand`와 `OutboundMessage`를 제공한다.

완료 기준: 동일한 기능·안정성·부하 테스트를 `epoll`과 `io_uring` backend에 모두 적용하고
결과를 비교할 수 있다.

## 6. 라이브 콘텐츠 Vertical Slice

PlayerActor에 다음 콘텐츠를 C++로 구현한다.

- 성장과 업적
- 기간제 이벤트
- 상점 구매와 재화 차감
- 멱등한 보상 지급
- 랭킹 점수 반영과 시즌 정산

DB transaction, entity version, idempotency key, 비동기 저장과 운영 중 기능 비활성화를 함께
검증한다. 필요하면 C# 운영 툴은 이 단계 이후에 추가한다.

완료 기준: 중복 요청, DB 지연·실패, 재시작과 이벤트 종료 상황에서도 재화와 보상 무결성이
유지된다.

## 7. 최소 World와 Battle

World와 Battle은 아키텍처 및 상태 전환을 검증할 수 있는 범위로 구현한다.

- World: Zone, 입력 기반 이동, 기본 충돌, 전투 진입
- Battle: CombatRoom, fixed tick, 이동·충돌, 종료 조건
- World → Battle → World 간 SessionRoute 변경
- CombatRoom의 BattleResult를 PlayerActor가 멱등하게 적용
- tick 실행 시간과 overrun metric

완료 기준: 유저가 World에서 전투에 진입하고 결과를 적용받은 뒤 안전하게 World로 복귀한다.

## 공통 원칙

- Network Thread는 게임 상태를 수정하지 않는다.
- Handler는 물리 Thread가 아니라 상태 소유 Actor를 선택한다.
- 다른 Runtime의 mutable 객체를 직접 참조하지 않는다.
- 모든 비동기 queue는 bounded queue로 만든다.
- Service Layer는 필요가 생기기 전까지 별도로 만들지 않는다.
- 측정 전에는 lock-free, microservice, Actor 내부 병렬화를 도입하지 않는다.
