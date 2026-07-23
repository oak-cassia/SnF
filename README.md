# SnF

C++20 기반 실시간 게임 서버 연습 프로젝트다. 첫 단계에서는 Linux `epoll`
기반 TCP 서버의 기반과 부하 테스트 클라이언트를 만든다.

## 개발 환경 실행

이미지를 한 번 빌드한다.

```bash
docker build -t snf-server-dev .
```

프로젝트 폴더를 연결한 개발 컨테이너를 실행한다.

```bash
docker run --rm -it \
  -p 7777:7777 \
  -v "$PWD:/workspace" \
  -w /workspace \
  snf-server-dev
```

컨테이너 안에서 Debug 빌드와 테스트를 실행한다.

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

ASan과 UBSan 검증은 별도 build directory를 사용한다.

```bash
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan
```

## 현재 실행 파일

- `snf_server`: 이후 `epoll` TCP 서버가 될 실행 파일
- `snf_load_client`: 이후 다중 접속 부하 테스트 클라이언트가 될 실행 파일

현재는 CMake와 Docker 개발 환경을 확인하기 위한 최소 scaffold만 구현되어 있다.
