# syntax=docker/dockerfile:1

FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG USER_ID=10001
ARG GROUP_ID=10001

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        clang-format \
        clang-tidy \
        cmake \
        gdb \
        git \
        iproute2 \
        libclang-rt-dev \
        libhiredis-dev \
        libprotobuf-dev \
        libssl-dev \
        lldb \
        default-libmysqlclient-dev \
        default-mysql-client \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        redis-tools \
        valgrind \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid "${GROUP_ID}" game-server \
    && useradd \
        --create-home \
        --gid "${GROUP_ID}" \
        --shell /bin/bash \
        --uid "${USER_ID}" \
        game-server

WORKDIR /workspace

COPY --chown=game-server:game-server . .
RUN chown game-server:game-server /workspace

USER game-server

ENV ASAN_OPTIONS="abort_on_error=1:detect_leaks=1:strict_string_checks=1" \
    UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
    TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1"

EXPOSE 7777/tcp

STOPSIGNAL SIGTERM

CMD ["/bin/bash"]
