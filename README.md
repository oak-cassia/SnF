# SnF

SnF는 C++20을 활용해 실시간 게임 콘텐츠의 상태, 규칙과 메시지 흐름을 설계하고 구현하는
프로젝트다. 서버 코어 자체를 계속 확장하는 것보다, Player와 Zone을 비롯한 게임 콘텐츠를
명확한 상태 소유권과 실행 경계 위에 올리는 것을 주된 목적으로 한다.

현재는 Linux `epoll` 네트워크 런타임과 sharded Actor Runtime 위에서 PING/PONG vertical
slice를 실행한다. 이 기반을 일반화한 뒤 인증·영속성, Zone과 timer event, 공유 콘텐츠를
차례로 구현한다.

## 프로젝트 목적

- 현대적인 C++로 게임 상태와 콘텐츠 규칙을 모델링한다.
- Player, Zone과 공유 콘텐츠를 Actor 단위의 순차 실행으로 구성한다.
- 네트워크, 게임 로직과 느린 외부 I/O 사이의 경계를 명시한다.
- 인증·영속성·이동·AOI·공유 콘텐츠를 작은 vertical slice로 구현하고 검증한다.
- 기능뿐 아니라 순서 보장, 수명, backpressure와 graceful shutdown까지 함께 다룬다.

## 개발 방식

프로젝트의 주된 학습·설계 영역은 C++을 활용한 게임 콘텐츠다. 콘텐츠의 상태 소유권,
규칙, 메시지와 처리 흐름은 직접 설계하고 구현한다.

서버 코어는 먼저 전체 아키텍처, public contract, 불변 조건과 완료 기준을 정한다. 그 경계
안에서 반복적인 세부 구현, 테스트 보강, 리팩터링과 문서 정리는 LLM에 위임한다. LLM이 만든
변경은 코드 리뷰와 단위·통합 테스트, sanitizer 및 필요한 부하 테스트를 통과한 뒤 받아들인다.

| 영역 | 중점 |
| --- | --- |
| 게임 콘텐츠 | 상태 모델, 규칙, 메시지 흐름과 C++ 구현 |
| 서버 코어 | 전체 방향, 계층 경계, 실행·수명·포화 계약 설계 |
| LLM 활용 | 정해진 계약 안의 세부 구현, 테스트, 리팩터링과 문서화 |

## 아키텍처

```text
Client
  ↓
epoll Network Runtime
  ↓ FrameEnvelope
ProtocolGateway / CommandRouter
  ↓ typed route
Actor-Bound Logic Runtime
  ├── PlayerActor
  └── ZoneActor, Shared Content Actor (예정)
  ↓ typed effect
OutboundSink
  ↓
Network Runtime
```

핵심 원칙은 다음과 같다.

- Network thread는 게임 상태를 직접 수정하지 않는다.
- 각 Actor의 mutable 상태는 하나의 Logic Worker에서만 순차적으로 변경한다.
- protocol Frame을 Actor까지 전달하지 않고 typed command와 effect 경계를 사용한다.
- queue와 in-flight operation에는 명시적인 상한과 포화 정책을 둔다.
- 외부 executor는 Actor 객체나 coroutine handle을 보유하지 않는다.
- 종료는 ingress close, Actor drain, pending send drain 순서를 명시적으로 따른다.

## 현재 상태

- non-blocking TCP listener와 level-triggered `epoll` reactor
- 길이 기반 binary Frame codec과 부분 수신·송신 처리
- 공통 `ProtocolGateway`와 typed command routing
- `ActorKey{ActorKind, EntityId}`로 sharding하는 2-Worker Actor-Bound Logic Runtime과 Actor별 FIFO mailbox
- Player 전용 `PlayerActorBinding`/`PlayerActorIngress`와 type-erased binding registry
- `PlayerActor` PING/PONG 처리와 typed result/effect 경계
- connection generation을 통한 stale response 차단
- bounded ingress/outbound queue와 Session별 send backpressure
- connection lifecycle 전달, runtime drain/failure와 graceful shutdown
- 단위·TCP 통합·부하 테스트 및 ASan·UBSan·TSan preset

Phase 3.8에서 scheduler의 Player 전용 의존을 제거하고, 모든 Worker를
`ActorKeyHash(key) % worker_count`로 선택하는 Actor-Bound Logic Runtime으로 일반화했다.
현재 production binding은 Player 하나이며 ZoneActor, timer와 coroutine은 이후 단계의 범위다.

## 로드맵

```text
3.7 Effect / Protocol / Lifecycle 경계 강화
→ 3.8 Actor-Bound Logic Runtime 일반화
→ 4 Coroutine Suspend / Resume
→ 5 인증·영속성
→ 6 ZoneActor와 Timer Event
→ 7 Shared Content와 Projection
```

상세 단계와 완료 기준은 [개발 로드맵](docs/development-roadmap.md), 목표 구조와 상태 소유권은
[서버 아키텍처 초안](docs/server-architecture-draft.md), coroutine 수명 규약은
[Coroutine Actor 계약](docs/coroutine-actor-contract.md)을 기준으로 한다.

## 빌드와 테스트

서버는 Linux `epoll`을 사용한다. macOS에서는 Ubuntu 24.04 Docker container 안에서 빌드하고
실행한다.

```bash
docker build -t snf-server-dev .

docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev
```

Debug 빌드와 테스트:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

Sanitizer 검증:

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan --output-on-failure

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

Release 빌드:

```bash
cmake --preset release
cmake --build --preset release
```

## 실행

서버:

```bash
./build/release/snf_server
```

부하 테스트 클라이언트:

```bash
./build/release/snf_load_client \
  --host 127.0.0.1 \
  --port 7777 \
  --connections 1000 \
  --duration 30 \
  --requests-per-second 10
```

현재 wire format은 다음과 같다.

```text
[body_length:u32][type:u16][request_id:u32][payload]
```

모든 정수는 big-endian이며 body는 최대 64 KiB다. 현재 메시지는 `PING=1`, `PONG=2`를
사용한다.

## 디렉터리

| 위치 | 내용 |
| --- | --- |
| `include/snf/`, `src/` | core, network, protocol과 server runtime |
| `tests/` | 단위·통합 테스트 |
| `tools/load_client/` | non-blocking 부하 테스트 클라이언트 |
| `docs/` | 아키텍처, 로드맵과 coroutine 계약 |
