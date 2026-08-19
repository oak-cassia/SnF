#pragma once

#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/tell_payload.hpp"

#include <chrono>
#include <variant>

namespace snf::server
{
    // What an actor asks the runtime to do once its handler has returned. These
    // are deliberately the runtime-owned destinations only -- another actor's
    // mailbox and this Worker's timer heap. A response travels to the outbound
    // channel instead, which is a server resource the runtime knows nothing
    // about, and whose type differs per actor kind, so it is not an alternative
    // here.
    //
    // The runtime applies them after the handler returns normally, so a handler
    // that throws mid-way asks for nothing.

    // Fire-and-forget. There is no reply channel by design: an actor that waited
    // for another actor could form a cycle this runtime cannot detect or break.
    struct TellActor
    {
        snf::runtime::ActorKey target;
        snf::runtime::TellPayload payload;
    };

    // One-shot. Repetition is the actor asking again from its own handler, which
    // is what lets a state machine change or stop its own cadence.
    struct ScheduleTimer
    {
        std::chrono::milliseconds delay{0};
    };

    using FollowUpAction = std::variant<TellActor, ScheduleTimer>;
}
