# ConnectionScope 계약

> 상태: 보류된 선택적 Runtime 최적화 설계. 현재 콘텐츠 로드맵의 선행 조건이 아니며,
> Playable Session과 Zone 부하가 runtime 병목을 증명할 때 다시 검토한다.
> 범위: 최소 Executor 계약, 연결 하나의 네트워크 상태 소유권, 단일 종결과 close 순서,
> in-flight credit, runtime deadline과 heartbeat, 취소 전파
>
> Actor runtime 내부 조건은 [Coroutine Actor 계약](./coroutine-actor-contract.md)이,
> 서버 전체 종료 판정과 Runtime 사이의 조합은 [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)이,
> 목표 구조와 불변식은 [서버 아키텍처 초안](./server-architecture-draft.md)이,
> 구현 승격 관문과 순서는 [개발 로드맵](./development-roadmap.md)의 선택적 Runtime 최적화 절이 소유한다.

이 문서는 즉시 구현 지시가 아니라, 향후 네트워크 coroutine이 필요해졌을 때 수명 규칙을
다시 발명하지 않기 위한 설계 입력이다. 지금까지 종결 순서와
포화 정책은 reactor 구현 안에 흩어져 있었고, 장수명 coroutine을 도입하면 "누가 무엇을 파괴할 수 있는가"가
정확한 답을 요구하는 질문이 된다.

## 1. Executor 계약

`TcpServer::run()`은 coroutine을 재개하는 executor가 아니라 동기 epoll 루프였다. 이 단계는 그 루프를
최소 Executor로 바꾼다. 새 thread pool은 만들지 않고 기존 reactor thread가 executor를 실행한다.

### 1.1 turn 순서

```text
epoll_wait 반환
→ control (stop / termination signal / outbound / runtime completion)
→ deadline 만료 판정
→ ready queue drain (회차당 budget)
→ 종결 전이 (Draining → Closing)
→ retire (coroutine frame 파괴)
```

**budget이 걸리는 것은 ready queue뿐이다.** 그래서 ready queue가 포화해도 deadline과 control은 굶지
않는다. budget이 소진되면 다음 `epoll_wait` timeout을 0으로 두어 잔여 작업이 다음 회차로 넘어가되
blocking하지 않는다. 이것이 연결 폭주에서 이 loop이 진행성을 유지하는 방식이다.

readiness는 coroutine을 inline resume하지 않는다. epoll 이벤트는 awaiter에 readiness를 기록하고 ready
queue에 token을 게시할 뿐이며, 재개는 항상 drain 단계에서 일어난다.

### 1.2 ready queue의 hard bound

```text
ready queue 최대치 = 2 × max_scopes + 고정 control awaiter 수
```

`max_scopes`는 `connection_lifecycle_capacity`이고, scope당 최대치가 2인 것은 read loop과 write loop이
각각 최대 하나의 suspension point만 가지기 때문이다. 이 설계의 첫 구현에서 control은 coroutine이 아니라 turn
앞단에서 직접 처리하므로 고정 control awaiter 수는 0이다. 통합 runtime에서 control이 awaiter가 되면 그 고정 수를
더한다.

이 최대치는 생성 시점에 고정 용량 ring buffer로 잡는다. `reserve()`는 할당을 줄일 뿐 상한이 아니므로
bound로 쓰지 않는다. 초과는 재할당이 아니라 불변식 위반이며 `std::logic_error`로 드러낸다.

`2 × max_scopes`는 checked multiplication으로 계산한다. `connection_lifecycle_capacity`는 설정값이므로
곱이 `std::size_t`를 넘는 구성은 생성 시점에 거부한다. 상한을 뜻하는 값이 조용히 감싸 돌면 그 상한이
보장하는 모든 것이 함께 사라진다.

이 상한이 실제로 지켜지려면 slot 회계를 scope 수명이 아니라 token의 물리 제거에 붙여야 한다. 그 규칙은
§1.3이 소유한다.

### 1.3 ready token은 coroutine handle이 아니다

**ready queue는 `std::coroutine_handle`을 저장하지 않는다.** `{ConnectionId, Side, SuspensionGeneration}`을
저장하고 drain 시점에 scope map으로 resolve한다. token은 값이므로 참조하는 대상이 사라져도 token 자체가
무효 포인터가 되지 않는다.

**resolve는 다음이 모두 참일 때만 성공한다.**

```text
token이 유효하다 =
    그 ConnectionId의 scope가 존재함
    && 해당 side가 아직 active함
    && 그 side의 awaiter가 queued 상태임
    && suspension generation이 일치함
```

scope가 `Draining`이나 `Closing`이면 map에는 그대로 남아 있으므로 **scope 존재만으로는 판정이 되지
않는다.** 취소된 side는 active가 아니므로 두 번째 조건에서 걸러진다. generation은 같은 side가 취소 후
다시 suspend한 경우를 구분한다.

**논리적 무효화로 충분하므로, 물리 token이 ready queue에 남아 있어도 retire를 허용한다.** stale token이
drain될 때까지 scope를 붙잡아 두면 종결이 ready queue의 소진 속도에 묶이고, use-after-free 관점에서
얻는 것이 없다. token이 값이라는 사실이 그 대기를 불필요하게 만든다.

이것은 epoll이 FD 재사용 뒤에 복사된 이벤트를 반환하는 문제를 generation token으로 폐기하는 기존 규율과
같다. `final_suspend = suspend_always`와 retire 순서도 함께 지키지만, 그것들은 이중 방어이지 유일한
보장이 아니다.

**다만 use-after-free와 용량 회계는 다른 문제다.** 물리 token을 남긴 채 retire하고 그 lifecycle slot을
새 scope가 재사용하면, 새 scope가 자기 token 2개를 게시해 §1.2의 상한을 넘을 수 있다. capacity 1에서
stale token 2개가 ring에 남은 채 retire하고 새 연결을 수락하면 그대로 overflow다. 그래서 slot 회계를
scope 수명이 아니라 **token의 물리 제거**에 붙인다.

- ring slot은 token이 게시될 때 소비되고, **물리적으로 제거될 때**(resume 또는 폐기) 반환된다. scope의
  retire는 그것을 반환하지 않는다.
- retire 시점에 ring에 남은 그 scope의 token 수를 `orphaned_ring_slots`로 회계한다. scope당 최대 2다.
- **새 scope 수락 조건에 그 항을 포함한다.**

```text
2 × live_scopes + orphaned_ring_slots + 2 ≤ ring capacity
```

이 조건이 거짓이면 accept를 미룬다. reactor는 이미 lifecycle slot이 없을 때 listener backlog를 다음
회차로 넘기므로, 새 backpressure 기구를 만드는 것이 아니라 기존 경로에 항 하나를 더하는 것이다.
orphan은 다음 drain 통과에서 해제되므로 지연은 회차 단위이고, 상한은 어떤 순간에도 깨지지 않는다.

**채택하지 않은 두 대안.** 물리 token이 drain될 때까지 lifecycle slot을 붙잡는 방식은 종결 지연을
ready queue 소진 속도에 결합시킨다. O(1) 물리 제거가 가능한 intrusive queue는 상한 문제를 구조적으로
없애지만, queue가 scope 메모리를 가리키게 되어 이 절이 확보한 value token 성질을 포기한다.

### 1.4 중복 게시와 clock

- awaiter마다 `queued` 플래그를 두고 false→true 전이에서만 post한다. suspension point당 ready token
  하나이며, Actor ready queue의 "Actor당 최대 하나의 token" 규칙과 같은 이유다.
- write loop의 token은 outbound action 도착과 `EPOLLOUT` 두 경로에서 게시될 수 있다. 같은 플래그가 두
  경로를 합치므로 한 회차에 두 번 재개되지 않는다.
- **clock은 turn당 두 번 읽는다.** `epoll_wait` 전에 timeout을 계산하기 위해, 반환 후에 만료를
  판정하기 위해. 반환 후의 값은 그 turn의 모든 timer 판정과 metric 표본이 공유하므로, timer 수만큼
  시간 조회가 늘지 않는다.

### 1.5 deadline heap과 재무장 churn

두 가지 성장 경로를 각각 막아야 한다.

**활동당 재무장.** inbound frame마다 idle deadline을 재무장하면 heap push가 frame 수만큼 발생한다.

- **scope·목적당 등록된 deadline은 최대 1개다.** 목적은 idle timeout과 heartbeat 두 가지이므로 scope당
  최대 2개다.
- 활동이 있을 때 재무장하지 않고 scope의 `last_activity_at`만 갱신한다.
- 만료 시 `now - last_activity_at`을 검사한다. 아직 idle이 아니면 남은 간격으로 **한 번** 재무장한다.

따라서 heap push는 frame당이 아니라 timeout 간격당 1회다.

**연결 churn.** 위 규칙은 *등록된* deadline만 제한하므로 물리 heap node의 수를 제한하지 않는다. 긴
deadline을 등록한 연결이 곧바로 종료되기를 반복하면, 무효화된 node가 원래 만료 시각까지 heap에 남아
계속 쌓인다. 한 시간짜리 deadline과 초당 수백 건의 연결 churn이면 그 축적이 실제 문제가 된다.

그래서 **deadline heap은 indexed heap이며 cancel과 update가 물리 node를 즉시 제거한다.** scope가 자신의
deadline handle을 보유하고, retire 전에 물리 제거를 완료한다. tombstone 예산 + bounded compaction도
가능한 대안이지만 상한을 두 곳에 두게 되므로 채택하지 않는다.

generation은 남겨 두되 역할이 바뀐다. 물리 제거가 1차 보장이고 generation은 이미 pop된 만료를 뒤늦게
처리하는 경로에 대한 2차 검사다.

## 2. 상태 소유권

게임 상태는 여전히 Actor가 소유하고 `ConnectionScope`는 네트워크 상태만 소유한다.

| 소유자 | 상태 |
| --- | --- |
| read loop | recv buffer, frame assembler, credit 취득 gate |
| write loop | send buffer의 소비와 `EPOLLOUT` 재무장, pending send 진행 상태 |
| scope | fd, `ConnectionId`(불변), epoll interest mask, 종료 상태, `last_activity_at`, heartbeat 상태 |

- **interest mask는 scope가 소유한다.** 양쪽이 `wantRead()`/`wantWrite()`를 선언하고 scope가 재계산해
  `epoll_ctl`을 한 곳에서 호출한다. epoll 등록 소유자를 둘로 만들지 않기 위함이다.
- **send buffer는 소비와 생산을 구분한다.** write loop이 소유하는 것은 소비와 재무장이며, enqueue는
  scope가 노출하는 생산 진입점이다. outbound action을 적용하는 주체는 turn의 control 단계이므로,
  "write loop만 send buffer를 만진다"가 아니라 "소비 측 상태 전이는 write loop에만 있다"가 정확한
  서술이다.
- **heartbeat 상태는 어느 한 loop의 소유가 아니다.** write loop이 방출하고 read loop이 해제한다. 두
  loop이 공유하는 것은 scope가 소유한 상태뿐이라는 규칙을 유지하기 위해 `last_activity_at`과 같은 층에
  둔다. 두 loop이 같은 스레드에서 실행되므로 이는 동기화 서술이 아니라 소유권 서술이다.

## 3. 단일 종결

```text
Open → Draining(cause) → Closing → Retired
```

read loop, write loop, deadline과 control이 모두 `requestClose(cause)`를 호출할 수 있지만 전이는
`ConnectionScope`만 수행한다.

- `requestClose(cause)`는 첫 원인만 기록하고 멱등이며 **전이하지 않는다.** 즉시 하는 일은 신규 read
  admission 차단 하나뿐이다.
- 전이는 turn의 종결 처리 단계에서만 수행한다. **그 시점의 read loop은 suspend 또는 완료 상태이므로
  진행 중인 ingress 게시가 존재하지 않는다.** 이것이 `ConnectionClosed`가 전이 전에 승인된 command
  뒤에 온다는 것의 증명이며, ingress가 `Full`을 반환해 그 자리에서 `requestClose`가 불리는 경우도 같은
  규칙으로 처리된다.
- **`Draining` 진입 시 취소 범위는 cause가 결정한다.** 두 side를 항상 함께 취소하면 §3.2의 shutdown
  flush와 충돌한다.
  - 일반 종료(`PeerClosed`·`ProtocolError`·`Overflow`·`Timeout`): read와 write awaiter를 모두 취소한다.
  - `ServerShutdown`: read awaiter만 취소해 read loop을 완료시키고 **write loop은 유지한다.** Logic
    Runtime이 drained에 도달하고 send queue가 빈 시점, 또는 grace deadline 중 먼저 오는 쪽까지
    write-side graceful drain을 계속한다.
  - 어느 경우든 취소된 side의 queued ready token은 §1.3의 유효성 술어에서 걸러진다.
- 첫 단일-reactor 구현에서 `requestClose`는 reactor thread에서만 호출된다. Worker가 관측한 outbound admission
  실패도 reactor가 기록을 소비해 호출하므로, 단일 종결의 증명은 CAS가 아니라 한 스레드 위의 상태
  기계다.

### 3.1 게시 불변식

> `ConnectionClosed`는 최대 한 번 게시된다. graceful shutdown을 포함해 ingress close 이전에
> `Draining`에 진입한 scope는 정확히 한 번 승인된다. 포화 시 lifecycle slot에 보존해 재시도하고,
> 모든 lifecycle이 승인된 뒤에만 Logic ingress를 닫는다.

grace deadline 만료나 runtime 실패처럼 cancel로 전환된 경로에서는 승인을 보장하지 않는다. 이 경계는
[Runtime Lifecycle 계약](./runtime-lifecycle-contract.md) §3·4와 같다.

### 3.2 파괴 조건과 close 순서

**scope 파괴는 socket close와 같은 사건이 아니다.** `Closing`은 `ConnectionClosed`가 종결될 때까지
유지되고, lifecycle slot 예산은 이 상태에 붙는다.

파괴는 다음이 모두 참일 때만 한다.

```text
두 loop이 요청 수신이 아니라 완료했음
&& ConnectionClosed가 종결됐음 (게시 승인 또는 게시하지 않기로 결정)
&& 등록된 deadline의 물리 heap node가 제거됐음
```

조건이 **아닌** 것 둘을 함께 적는다.

- **ready queue에 이 scope의 token이 남아 있는지.** §1.3의 논리적 무효화가 그 대기를 불필요하게 만든다.
  대신 그 token이 점유한 ring slot은 §1.3의 `orphaned_ring_slots`로 회계되어 새 scope 수락을 미룬다.
  파괴를 막는 것과 slot을 반환하는 것을 분리했다는 뜻이다.
- **그 연결의 credit state가 회수됐는지.** credit state의 수명은 scope 수명과 분리되어 있다(§4.1). 이것을
  파괴 조건에 넣으면 연결의 teardown이 Worker 쪽 command 완료에 묶이고, detach로 끊어낸 결합이 그대로
  되살아난다. lifecycle slot도 그만큼 오래 붙잡힌다.

cause별 linger는 현재 동작을 유지한다. `ServerShutdown`은 grace period 안에서 pending send를 flush하고,
`Overflow`·`ProtocolError`·`PeerClosed`·`Timeout`은 즉시 종료한다. §3의 cause별 취소 범위가 이 차이를
집행하는 쪽이다.

## 4. In-flight credit

### 4.1 소유자와 수명

credit 소유자를 `ConnectionId`로 키잉하면 release가 Worker thread에서 map을 조회하게 된다. release는
owning Worker가 scheduling mutex를 들고 실행하는 경로이므로 block·allocation·throw가 허용되지 않으며,
조회는 그 제약과 release-after-retire 위험을 함께 만든다. 따라서:

- **`ConnectionCreditState`의 수명은 `scope 수명 ∪ outstanding token 수명`이다.**
- **release 토큰은 state를 직접 가리킨다. 소유하지는 않는다.** release는 credit atomic 증가, coalesced
  wake-up 하나, 기존 counter 갱신뿐이다. 조회 없음, 락 없음, 할당 없음.
- **retire 시 state를 detached로 표시한다.** 이후 도착한 release는 counter만 갱신하고 wake-up을
  신호하지 않는다. 그래서 scope가 사라진 뒤의 늦은 release가 안전하다.
- **마지막 소유권과 파괴는 reactor가 담당한다.** 이것을 일반적인 refcount로 두면 마지막 참조를 버리는
  주체가 Worker가 될 수 있고, 그러면 deallocation이 release 경로에서 일어난다. allocator 진입은 그
  경로의 non-blocking 규율과 맞지 않는다. 따라서 소유 참조는 **항상 reactor 하나만** 보유하고, token은
  `outstanding` counter만 증감한다.
- **token의 마지막 동작은 `outstanding` 감소이며, 그 뒤로 state를 만지지 않는다.** reactor는
  `detached && outstanding == 0`을 관측한 뒤에만 회수한다. 그래서 Worker가 하는 일은 atomic 반환과
  wake-up뿐이고 파괴는 reactor의 turn 안에서만 일어난다.
- **Worker의 마지막 `outstanding` 감소는 release, reactor의 0 관측은 acquire다.** 그 짝이 없으면 회수가
  token이 state에 가한 마지막 쓰기보다 앞설 수 있다.
- **detached state의 수는 outstanding command 총상한 이하다.** state가 detached로 남는 유일한 이유가
  outstanding token이므로, 연결 churn이 아무리 빨라도 그 수는 Worker별 in-flight 예산의 합을 넘지 않는다.
  scope 수명과 분리하면서 무한 축적을 새로 만들지 않는다는 것이 이 항의 내용이다.

그래서 scope가 `Retired`가 된 뒤에도 그 연결의 credit state는 outstanding이 0이 될 때까지 reactor가
붙들고 있고, 회수는 그 뒤의 turn에서 이루어진다. 이 회수는 §3.2의 파괴 조건에 들어가지 않는다. 두 수명을
분리해 둔 이유가 그것이다.

### 4.2 취득 경계

취득은 **command를 승인하는 binding 경계**, 즉 release 토큰이 무장되는 바로 그 자리다. 두 수치가
일치해야 하므로 다른 경계를 쓰지 않는다.

조회 없이 그 경계에 닿게 하려면 credit state가 ingress 경로를 따라 함께 이동해야 한다. read loop은
자기 scope의 state를 이미 손에 들고 있으므로 조회가 0이고, binding이 그 state에서 lease를 만들어
command payload에 넣는다. protocol 단계에서 거부된 frame은 lease를 만든 적이 없으므로 일시적인 보유조차
없다.

**계층 제한:** 이렇게 이동하는 값은 domain이 해석하지 않는 opaque admission context이며, **routing DTO
까지만 지나간다.** `PlayerCommand`, `PlayerResult`, `PlayerActor`의 상태 어디에도 들어가지 않는다.
credit은 네트워크 층의 회계이고, domain Actor가 그것을 아는 순간 outbound 용량을 몰라야 한다는 4.1의
경계와 같은 종류의 누출이 된다. 어느 타입이 그 context를 나를지는 구현 단계에서 결정한다.

`ConnectionClosed` submission은 release 토큰을 갖지 않으므로 credit을 소비하지 않는다.

### 4.3 gating과 재개

- credit이 소진되면 socket 읽기를 중지한다. 디코드하지 않은 바이트는 recv buffer에 남고, 읽기를
  멈추면 TCP가 상대를 막으므로 **inbound frame을 조용히 버리는 경로가 없다.**
- **read loop의 gate는 같은 스레드 precheck이므로 정확하다.** 취득은 reactor thread만 수행하고 다른
  스레드는 credit을 증가시키기만 하므로, check 뒤 post가 상한을 넘길 수 없다.
- reactor는 읽기를 멈춘 scope를 자신의 gated list에 넣는다. release는 wake-up을 한 번 신호할 뿐이고,
  어느 연결인지는 reactor가 자기 gated list를 순회해 판정한다. 공유 리스트도 할당도 필요 없다.
- gated list 순회는 회차당 상한과 회전 규율을 쓴다. 상한에 막힌 연결을 뒤로 돌려 다른 연결을 막지
  않는 것은 outbound grant와 같은 규칙이다.

## 5. Deadline과 heartbeat

heartbeat와 idle timeout은 이 층의 runtime deadline primitive로 표현하며 gameplay domain
`TimerService`에 의존하지 않는다. 둘은 다른 층이다.

- `idle_timeout`과 `heartbeat_interval`은 모두 기본 0(비활성)이다. 기본 경로의 동작은 이 단계 전후로
  같다.
- 만료는 `requestClose(ConnectionCloseCause::Timeout)`이다. idle timeout과 heartbeat 미응답이 같은
  cause를 쓰며 새 cause를 더 만들지 않는다.

**heartbeat는 네트워크 계층에서 완결된다.**

- write loop이 `Ping`을 자기 send queue에 직접 방출한다. outbound channel을 거치지 않으므로 reservation
  slot도 연결별 outbound 상한도 소비하지 않는다. 상한은 `max_pending_send_bytes` 하나이며, heartbeat
  Ping이 들어갈 자리조차 없다는 것은 상대가 소비하지 않는다는 뜻이므로 기존 `Overflow` 종료가 옳은
  처리다.
- read loop이 inbound `Pong`을 **heartbeat 활성 시에만** 가로챈다. 그 frame은 protocol gateway에 도달하지
  않으므로 command가 되지 않고 credit도 actor turn도 소비하지 않는다. domain dispatcher와 `PlayerActor`는
  이 기능을 알지 않는다.
- 비활성 시 inbound `Pong`은 현재 동작을 보존한다. dispatcher가 `Pong` handler를 등록하지 않으므로
  지원하지 않는 메시지로 처리되어 연결이 `ProtocolError`로 종료된다.
- `request_id`가 일치하는 `Pong`은 outstanding heartbeat를 해제한다. 불일치하는 `Pong`은
  `last_activity_at`만 갱신하고 해제하지 않으며 종료 사유가 되지 않는다. metric으로만 관측한다.

### 5.1 heartbeat 상태 기계

**heartbeat deadline handle은 하나이고 상태마다 갱신된다.** 모든 상태에 시간 상한이 있어야 한다.

```text
Idle
  ── interval 만료 ──────────→ Queued
                                 (Ping을 send queue에 넣고, deadline을 send timeout으로 갱신)
Queued
  ── 그 Ping의 마지막 byte가 커널에 전달 ──→ Awaiting
                                 (같은 deadline을 response timeout으로 갱신)
  ── send timeout 만료 ──────→ requestClose(Timeout)
Awaiting
  ── request_id 일치 Pong ───→ Idle
                                 (같은 deadline을 다음 interval로 갱신)
  ── response timeout 만료 ──→ requestClose(Timeout)
```

`Queued`에도 상한이 필요한 이유는 `max_pending_send_bytes`가 공간 상한이지 진행성 보장이 아니기
때문이다. 상대가 읽지 않고 Ping 하나만 queue에 남으면 그 상한에 닿지 않고, 송신 완료가 없으니 response
deadline도 무장되지 않는다. idle timeout까지 비활성이면 `Queued`가 영구히 지속된다.

handle이 하나이므로 scope당 deadline 목적 수는 idle과 heartbeat 둘, 최대 2개로 §1.5와 같다. indexed
heap에서 갱신은 node 하나의 위치 변경이며 새 node를 만들지 않는다.

설정값은 다음과 같다.

| 값 | 기본 | 의미 |
| --- | --- | --- |
| `heartbeat_interval` | 0 (비활성) | `Idle`에서 다음 Ping까지 |
| `heartbeat_timeout` | `heartbeat_interval`과 같음 | `Queued`와 `Awaiting` 각 단계의 상한 |

두 단계가 각각 `heartbeat_timeout`을 받으므로 무응답 상대의 최악 검출 시간은
`heartbeat_interval + 2 × heartbeat_timeout`이다. 기본값에서는 interval의 3배다. 한 deadline으로 두
단계를 함께 덮으면 그 값이 작아지지만, 응답 시간을 송신 완료 기준으로 재는 성질을 잃는다.

그 밖에 세 가지를 고정한다.

- **outstanding Ping은 최대 1개다.** `Awaiting` 중에 interval이 만료해도 새 Ping을 만들지 않는다. 그래서
  응답하지 않는 상대에게 Ping이 쌓이지 않고, `max_pending_send_bytes`가 heartbeat 때문에 소진되는 경로가
  없다.
- **response timeout은 enqueue가 아니라 실제 송신 완료 시점부터 센다.** enqueue 시점부터 세면 send queue에
  머문 시간이 응답 시간에 포함되어, 우리 쪽 적체를 상대의 무응답으로 오탐한다. 그 적체의 공간 상한은
  `max_pending_send_bytes`와 `Overflow`가, 시간 상한은 위의 send timeout이 담당한다.
- **interval 재계산은 `Awaiting`이 해제된 시점부터 한다.** 송신 시점부터 세면 응답이 느릴 때 interval과
  response timeout이 서로를 앞지른다.

`Queued` 상태에서 연결이 종결되면 그 Ping은 별도 처리 없이 사라진다. 상대가 관측한 적이 없고, 무장된
heartbeat deadline은 종결 경로가 §3.2의 물리 제거로 회수한다.

## 6. 취소 전파

- `Draining` 진입은 §3의 cause별 범위로 awaiter를 취소하고, 취소된 side의 queued token은 §1.3의 유효성
  술어에서 "side가 active함"에 걸려 폐기된다. scope가 map에 남아 있어도 그 판정은 성립한다.
- coroutine frame 파괴는 retire 단계에서만, executor를 실행하는 스레드에서만 일어난다. loop 자신의
  resume 안에서 자기 frame을 파괴하는 경로를 만들지 않는다.
- reactor가 종료하는 모든 경로는 outbound channel을 취소해야 한다. 이는 이 문서가 새로 만드는 규칙이
  아니라 [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md) §3의 역방향 의존을 그대로 따르는
  것이다. credit 반환이 멈추는 것도 같은 성질을 갖는다.
- **종료 경로에서 gated scope는 read 재개를 포기하지만 write-side drain은 유지한다.** reactor가 credit을
  소비·반환하지 않으면 읽기는 재개되지 않으므로 credit 반환을 기다리지 않는다. 그러나 이미 승인된
  command의 응답은 아직 outbound를 지나 나가야 하므로, `ServerShutdown`에서는 §3의 write-side graceful
  drain이 그대로 적용된다. read를 포기하는 것과 write를 끊는 것은 다른 결정이다.

## 7. Drain 판정

`NetworkRuntimeDrained`의 `no active connection`은 이 단계 이후 "모든 `ConnectionScope`가 `Retired`"를
뜻한다. 그 재정의는 [Runtime Lifecycle 계약](./runtime-lifecycle-contract.md)이 소유하며, 구현이
반영되는 시점에 갱신한다.

별도 `RuntimeId::Net`은 추가하지 않는다. executor가 reactor thread에서 실행되므로 network drain을
관측해야 하는 다른 스레드가 없고, 한 단계만 쓰이는 identity를 미리 확정하지 않는다는 기존 판단을
유지한다. 이 결정을 뒤집는 조건은 선택적 통합 runtime에서 network drain을 coordinator가
관측해야 할 때다.

## 8. 필수 경합 테스트

- credit 소진 상태에서 읽기 중지와 release 후 재개. **첫 command의 terminal을 막아 두 번째 command가
  release 전에 승인되지 않음을 확인한다.** 그래야 gating 자체가 증명된다.
- 배치 중간의 protocol error. 앞선 frame이 이미 승인된 상태에서 `ConnectionClosed`가 그 뒤에 한 번.
- 활성 연결을 둔 shutdown에서 `ServerShutdown` lifecycle이 한 번 승인되고 Player snapshot이 저장됨.
- **`ServerShutdown` 중 write-side drain 유지.** read는 취소되고 write는 이미 승인된 command의 응답을
  끝까지 내보낸다. grace deadline이 먼저 오는 경우도 함께 확인한다.
- coroutine frame 파괴, socket close, deadline 만료가 같은 turn에 경합.
- write loop이 suspend된 상태에서의 종결.
- retire 이후 도착한 credit release. **파괴가 reactor turn에서만 일어나는지**까지 확인한다.
- **긴 deadline을 등록한 연결의 즉시 종료를 반복해도 물리 heap node가 쌓이지 않음.**
- **ring 상한이 retire 후 slot 재사용에서 깨지지 않음.** ring capacity를 최소로 강제한 구성에서 stale
  token을 남긴 채 retire하고 곧바로 새 연결을 수락시켜, overflow 대신 accept 지연이 나타나는지 확인한다.
- **heartbeat 상태 기계**: `Awaiting` 중 interval 만료가 두 번째 Ping을 만들지 않음, response timeout이
  enqueue가 아니라 송신 완료에서 시작됨, interval이 `Awaiting` 해제 시점부터 다시 셈, **상대가 읽지 않아
  Ping 하나가 send queue에 머무는 경우 send timeout으로 종료됨**.
- stale generation ready token과 stale epoll 이벤트. **취소된 side의 token이 `Closing` scope에서도
  걸러지는지** 포함.
- partial send와 `EPOLLOUT` 재무장이 write loop 안에서 처리됨.
- 동시에 도착한 `EPOLLIN | EPOLLOUT | HUP`.
- deadline 재무장 churn이 heap 항목을 쌓지 않음.
- 연결 폭주와 강제 종료.

Debug와 ASan·UBSan 외에 별도 TSan 구성을 이 선택적 최적화의 완료 gate로 사용한다.
