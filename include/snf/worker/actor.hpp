#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/worker_event.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace snf::worker
{
    enum class ActorState : std::uint8_t
    {
        Idle = 0,
        Queued = 1,
        Running = 2,
        Stopping = 3,
    };

    enum class DeliveryResult : std::uint8_t
    {
        Accepted = 0,
        WrongOwner = 1,
        ActorTableFull = 2,
        MailboxFull = 3,
        ConstructionRejected = 4,
        Stopping = 5,
        Closed = 6,
    };

    struct ActorHandle
    {
        std::uint32_t slot_index{0};
        ActorIncarnation incarnation{};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return incarnation.isValid();
        }

        [[nodiscard]] bool operator==(const ActorHandle&) const noexcept = default;
    };

    struct ActorEnvelope
    {
        std::optional<ConnectionRef> connection{};
        snf::protocol::Frame frame{};
        std::uint32_t charged_bytes{0};

        [[nodiscard]] static ActorEnvelope fromFrame(
            const ConnectionRef connection,
            snf::protocol::Frame&& frame,
            const std::uint32_t charged_bytes = 0
        )
        {
            const std::uint32_t charge = (charged_bytes != 0)
                ? charged_bytes
                : static_cast<std::uint32_t>(frame.payload.size() + snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE);
            return ActorEnvelope{
                .connection = connection,
                .frame = std::move(frame),
                .charged_bytes = charge,
            };
        }

        [[nodiscard]] static ActorEnvelope fromFrame(
            snf::protocol::Frame&& frame,
            const std::uint32_t charged_bytes = 0
        )
        {
            const std::uint32_t charge = (charged_bytes != 0)
                ? charged_bytes
                : static_cast<std::uint32_t>(frame.payload.size() + snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE);
            return ActorEnvelope{
                .connection = std::nullopt,
                .frame = std::move(frame),
                .charged_bytes = charge,
            };
        }

        [[nodiscard]] bool operator==(const ActorEnvelope&) const noexcept = default;
    };

    class Mailbox final
    {
    public:
        Mailbox() = default;

        [[nodiscard]] bool empty() const noexcept
        {
            return _messages.empty();
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return _messages.size();
        }

        [[nodiscard]] std::uint64_t chargedBytes() const noexcept
        {
            return _charged_bytes;
        }

        void push(ActorEnvelope&& envelope)
        {
            _charged_bytes += envelope.charged_bytes;
            _messages.push_back(std::move(envelope));
        }

        [[nodiscard]] ActorEnvelope pop()
        {
            if (_messages.empty())
            {
                throw std::logic_error{"Mailbox underflow"};
            }
            ActorEnvelope env = std::move(_messages.front());
            _messages.pop_front();
            _charged_bytes -= env.charged_bytes;
            return env;
        }

        void clear() noexcept
        {
            _messages.clear();
            _charged_bytes = 0;
        }

    private:
        std::deque<ActorEnvelope> _messages;
        std::uint64_t _charged_bytes{0};
    };

    struct ActorTurnContext
    {
        ActivationRef activation;
        std::chrono::steady_clock::time_point now;
    };

    struct SendFrameEffect
    {
        ConnectionRef connection;
        snf::protocol::Frame frame;
        bool critical{false};

        [[nodiscard]] bool operator==(const SendFrameEffect&) const noexcept = default;
    };

    struct CloseConnectionEffect
    {
        ConnectionRef connection;
        CloseReason reason{CloseReason::Application};
        bool graceful{true};

        [[nodiscard]] bool operator==(const CloseConnectionEffect&) const noexcept = default;
    };

    struct TellActorEffect
    {
        ActorKey target;
        ActorEnvelope message;

        [[nodiscard]] bool operator==(const TellActorEffect&) const noexcept = default;
    };

    struct StopActorEffect
    {
        [[nodiscard]] bool operator==(const StopActorEffect&) const noexcept = default;
    };

    using Effect = std::variant<
        SendFrameEffect,
        CloseConnectionEffect,
        TellActorEffect,
        StopActorEffect>;

    class EffectBatch final
    {
    public:
        static constexpr std::size_t MAX_EFFECTS = 64;

        EffectBatch() = default;

        [[nodiscard]] bool tryPush(Effect effect)
        {
            if (_effects.size() >= MAX_EFFECTS)
            {
                return false;
            }
            _effects.push_back(std::move(effect));
            return true;
        }

        void push(Effect effect)
        {
            if (!tryPush(std::move(effect)))
            {
                throw std::overflow_error{"EffectBatch capacity exceeded (maximum 64 effects)"};
            }
        }

        [[nodiscard]] bool empty() const noexcept
        {
            return _effects.empty();
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return _effects.size();
        }

        [[nodiscard]] std::span<const Effect> effects() const noexcept
        {
            return _effects;
        }

        [[nodiscard]] std::span<Effect> mutableEffects() noexcept
        {
            return _effects;
        }

        void clear() noexcept
        {
            _effects.clear();
        }

    private:
        std::vector<Effect> _effects;
    };

    struct CompletedTurn
    {
        EffectBatch effects;
    };

    using TurnResult = std::variant<CompletedTurn>;

    class ActorInstance
    {
    public:
        virtual ~ActorInstance() = default;
        [[nodiscard]] virtual TurnResult dispatch(ActorEnvelope&& envelope, const ActorTurnContext& context) = 0;
    };

    struct ActorConstructionResult
    {
        enum class Status : std::uint8_t
        {
            Ready,
            Rejected,
        };

        Status status{Status::Rejected};
        std::unique_ptr<ActorInstance> instance{nullptr};

        [[nodiscard]] static ActorConstructionResult ready(std::unique_ptr<ActorInstance> actor)
        {
            if (!actor)
            {
                throw std::invalid_argument{"ActorConstructionResult::ready requires non-null actor instance"};
            }
            return ActorConstructionResult{
                .status = Status::Ready,
                .instance = std::move(actor),
            };
        }

        [[nodiscard]] static ActorConstructionResult rejected() noexcept
        {
            return ActorConstructionResult{
                .status = Status::Rejected,
                .instance = nullptr,
            };
        }

        [[nodiscard]] bool isReady() const noexcept
        {
            return status == Status::Ready && instance != nullptr;
        }

        [[nodiscard]] bool isRejected() const noexcept
        {
            return status == Status::Rejected;
        }
    };

    class ActorFactory
    {
    public:
        virtual ~ActorFactory() = default;
        [[nodiscard]] virtual ActorConstructionResult construct(ActorKey key) = 0;
    };

    struct WorkerActorConfig
    {
        std::size_t actor_table_capacity{16384};
        std::size_t max_mailbox_messages_per_actor{1024};
        std::uint64_t max_mailbox_bytes_per_actor{4ull * 1024 * 1024};
        std::size_t max_mailbox_messages_total{65536};
        std::uint64_t max_mailbox_bytes_total{64ull * 1024 * 1024};
        std::size_t max_turns_per_actor_slice{30};
        std::uint64_t placement_seed{0};
        std::chrono::milliseconds worker_shutdown_timeout{2000};
    };

    [[nodiscard]] inline bool isValid(const WorkerActorConfig& config) noexcept
    {
        return config.actor_table_capacity > 0 &&
               config.max_mailbox_messages_per_actor > 0 &&
               config.max_mailbox_bytes_per_actor > 0 &&
               config.max_mailbox_messages_total >= config.max_mailbox_messages_per_actor &&
               config.max_mailbox_bytes_total >= config.max_mailbox_bytes_per_actor &&
               config.max_turns_per_actor_slice > 0 &&
               config.worker_shutdown_timeout >= std::chrono::milliseconds::zero();
    }

    struct WorkerActorMetrics
    {
        std::uint64_t actor_turns{0};
        std::uint64_t stale_ready_handles{0};
        std::uint64_t budget_stops{0};
        std::uint64_t stopped_actors{0};
        std::uint64_t discarded_mailbox_messages{0};
        std::uint64_t discarded_mailbox_bytes{0};
        std::uint64_t effect_send_failures{0};
        std::uint64_t effect_close_failures{0};
        std::uint64_t effect_tell_failures{0};
        std::uint64_t wrong_owner_tells{0};
        std::uint64_t construction_rejections{0};
        std::uint64_t total_slice_duration_ns{0};
        std::chrono::nanoseconds max_slice_duration{0};
    };
}
