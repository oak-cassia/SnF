#pragma once

#include "snf/net/unique_file_descriptor.hpp"

namespace snf::worker
{
    // WakeupHandle 계약:
    // - 소유권 & 수명: WakeupHandle은 자신을 참조하는 모든 WorkerInboxPort보다 오래 살아야 한다.
    // - notify(): 다른 producer thread에서 호출 가능. throw하지 않고 blocking하지 않는다.
    //   write가 EAGAIN이면 counter가 포화(2^64-2)했다는 뜻이고 이미 wakeup이 걸려 있으므로 무시한다.
    //   EINTR이면 한 번 재시도한다.
    // - consume(): owner Worker thread 전용. 8바이트를 read하고 EAGAIN이면 무시한다.
    //   counter를 완전히 비우므로 반복 read가 필요 없다.
    // - 생성 실패 시에만 throw_system_error("eventfd")를 던지며, 그 외 경로에서는 throw하지 않는다.
    //
    // Coalescing 도입 시 주의사항 (MVP에서는 wake_pending flag 미도입):
    // producer enqueue -> wake_pending이 이미 true라 wake 생략
    // -> consumer가 뒤늦게 wake_pending=false
    // -> consumer sleep
    // -> item은 있는데 wakeup이 없음
    // 해결: drain 후 wake_pending=false로 내린 뒤 모든 lane을 acquire로 재확인한다.
    class WakeupHandle
    {
    public:
        WakeupHandle();

        WakeupHandle(const WakeupHandle&) = delete;
        WakeupHandle& operator=(const WakeupHandle&) = delete;

        [[nodiscard]] int descriptor() const noexcept;

        void notify() noexcept;
        void consume() noexcept;

    private:
        snf::net::UniqueFileDescriptor _fd;
    };
}
