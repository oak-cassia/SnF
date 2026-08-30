#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/identity.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <utility>

namespace snf::worker
{
    struct ActorEnvelope
    {
        std::optional<ConnectionRef> connection{};
        snf::protocol::Frame frame{};
        std::uint64_t charged_bytes{0};

        // The caller may request a larger logical charge, but it cannot
        // under-report the memory owned by the concrete Frame payload.
        [[nodiscard]] std::uint64_t chargedBytes() const noexcept
        {
            const auto frame_bytes = static_cast<std::uint64_t>(frame.payload.size()) +
                                     snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE;
            return std::max<std::uint64_t>(charged_bytes, frame_bytes);
        }

        [[nodiscard]] static ActorEnvelope fromFrame(
            const ConnectionRef connection,
            snf::protocol::Frame&& frame,
            const std::uint64_t charged_bytes = 0
        )
        {
            ActorEnvelope envelope{
                .connection = connection,
                .frame = std::move(frame),
                .charged_bytes = charged_bytes,
            };
            envelope.charged_bytes = envelope.chargedBytes();
            return envelope;
        }

        [[nodiscard]] static ActorEnvelope fromFrame(
            snf::protocol::Frame&& frame,
            const std::uint64_t charged_bytes = 0
        )
        {
            ActorEnvelope envelope{
                .connection = std::nullopt,
                .frame = std::move(frame),
                .charged_bytes = charged_bytes,
            };
            envelope.charged_bytes = envelope.chargedBytes();
            return envelope;
        }

        [[nodiscard]] bool operator==(const ActorEnvelope&) const noexcept = default;
    };
}
