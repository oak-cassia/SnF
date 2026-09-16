#pragma once

#include "snf/worker/identity.hpp"

namespace snf::worker
{
    // 불일치면 state를 바꾸지 않고 drop하며 metric만 올린다 (§9.4).

    // DB/adapter completion: 양쪽 identity가 유효하고 AwaitKey 전체가 일치할 때만 accept한다.
    [[nodiscard]] constexpr bool acceptsCompletion(const AwaitKey& arriving, const AwaitKey& blocked) noexcept
    {
        return arriving.incarnation.isValid() && arriving.operation.isValid() && blocked.incarnation.isValid() && blocked.operation.isValid() &&
               arriving == blocked;
    }

    // activation-bound timer/message: ActorSlot 조회로 ActorKey는 이미 맞았고 incarnation만 본다.
    [[nodiscard]] constexpr bool acceptsActivationEvent(const ActivationRef& event, const ActorIncarnation current) noexcept
    {
        return event.incarnation.isValid() && current.isValid() && event.incarnation == current;
    }

    // connection send/close: ConnectionSlot 조회로 id는 이미 맞았고 generation만 본다.
    // owner는 routing용이며 stale 판정에 쓰지 않는다.
    [[nodiscard]] constexpr bool acceptsConnectionAction(const ConnectionRef& target, const ConnectionGeneration current) noexcept
    {
        return target.generation.isValid() && current.isValid() && target.generation == current;
    }
}
