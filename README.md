# SnF

C++20과 Linux `epoll`로 만든 단일 스레드 실시간 TCP 서버 연습 프로젝트다. 현재 첫 번째
마일스톤인 PING/PONG 서버, graceful shutdown, non-blocking 부하 테스트 클라이언트와
1,000개 연결 승인 시험까지 완료했다.

## 현재 기능

### 서버

- non-blocking listener와 `accept4()`
- level-triggered epoll event loop
- RAII 기반 client FD와 Session 수명 관리
- 길이 기반 binary Frame decode
- 부분 수신과 여러 Frame 일괄 수신
- PING 요청에 request ID와 payload가 같은 PONG 응답
- 부분 송신 queue와 동적 `EPOLLOUT`
- Session별 기본 1 MiB 백프레셔
- client socket `TCP_NODELAY`
- `eventfd` 기반 `requestStop()`
- `signalfd` 기반 SIGINT·SIGTERM
- 기본 5초 pending send drain과 graceful shutdown

### 부하 테스트 클라이언트

- 여러 non-blocking 연결을 하나의 epoll에서 처리
- `EINPROGRESS`와 `getsockopt(SO_ERROR)` 연결 완료 확인
- `timerfd` 기반 연결별 요청 속도 제어
- 연결별 증가 request ID와 monotonic timestamp payload
- connect/request timeout
- 응답 type, request ID, payload 검증
- 연결·요청·오류·처리량 통계
- 평균 및 p50/p95/p99 RTT

## Wire format

```text
[body_length:u32][type:u16][request_id:u32][payload]
```

- 모든 정수는 big-endian이다.
- `body_length`는 길이 필드 자신을 제외한다.
- body는 최소 6 byte, 최대 64 KiB다.
- `PING=1`, `PONG=2`다.

## Docker 개발 환경

이미지를 한 번 빌드한다.

```bash
docker build -t snf-server-dev .
```

프로젝트를 mount한 개발 container를 실행한다.

```bash
docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev
```

macOS에는 epoll이 없으므로 서버와 부하 클라이언트 실행은 Ubuntu 24.04 container 안에서
수행한다.

## 빌드와 테스트

Debug 단위·통합 테스트:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

ASan·UBSan 검증:

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan --output-on-failure
```

Release 실행 파일:

```bash
cmake --preset release
cmake --build --preset release
```

Release preset은 `assert` 기반 테스트를 만들지 않고 실행 파일만 최적화해 빌드한다.

## 실행

첫 번째 terminal에서 서버를 실행한다.

```bash
./build/release/snf_server
```

두 번째 terminal에서 부하 클라이언트를 실행한다.

```bash
./build/release/snf_load_client \
  --host 127.0.0.1 \
  --port 7777 \
  --connections 1000 \
  --duration 30 \
  --requests-per-second 10 \
  --connect-timeout-ms 5000 \
  --request-timeout-ms 3000
```

CLI 기본값:

| 옵션 | 기본값 |
| --- | ---: |
| `--host` | `127.0.0.1` |
| `--port` | `7777` |
| `--connections` | `100` |
| `--duration` | `30`초 |
| `--requests-per-second` | 연결당 `1` |
| `--connect-timeout-ms` | `5000` |
| `--request-timeout-ms` | `3000` |

현재 host는 DNS 이름이 아닌 numeric IPv4 주소를 받는다.

## 첫 마일스톤 승인 결과

2026-07-26, Ubuntu 24.04 Docker container의 Release 빌드에서 다음 명령을 실행했다.

```text
Connections: 1000/1000 succeeded, 0 failed, peak active 1000
Requests: 300000 sent, 300000 received, 0 timeout, 0 invalid, 0 socket error
Throughput: 10000.000 responses/s
RTT ms: avg 9.705, p50 9.080, p95 13.697, p99 16.382
```

Debug 및 ASan·UBSan 구성에서도 단위 테스트, TCP loopback 테스트, 느린 client 백프레셔,
pending send graceful shutdown, load client 통합 테스트가 모두 통과했다.

## 주요 source

| 영역 | 위치 |
| --- | --- |
| Frame과 codec | `include/snf/protocol/`, `src/protocol/` |
| FD와 socket 설정 | `include/snf/net/`, `src/net/` |
| Session | `include/snf/net/session.hpp`, `src/net/session.cpp` |
| epoll 서버 | `include/snf/server/tcp_server.hpp`, `src/server/tcp_server.cpp` |
| 부하 클라이언트 | `include/snf/load/`, `src/load/`, `tools/load_client/` |
| 단위·통합 테스트 | `tests/` |

## 다음 단계

첫 마일스톤 이후에는 heartbeat와 idle timeout, 방 입장, 보스 전투, 방 단위 순차 실행,
worker thread 완료 queue와 TSan 검증 순으로 확장한다. MySQL, Redis, 재접속, C# 운영 도구는
그 이후 단계다.
