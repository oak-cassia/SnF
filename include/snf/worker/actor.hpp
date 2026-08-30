#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/actor_envelope.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/worker_event.hpp"

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace snf::worker
{
    using TimePoint = std::chrono::steady_clock::time_point;

    enum class ActorState : std::uint8_t
    {
        Idle = 0,
        Queued = 1,
        Running = 2,
        Suspended = 3,
        Loading = 4,
        Stopping = 5,
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
        ActivationLimit = 7,
        RemoteInboxFull = 8,
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
            const std::uint64_t charge = envelope.chargedBytes();
            if (charge > std::numeric_limits<std::uint64_t>::max() - _charged_bytes)
            {
                throw std::overflow_error{"Mailbox byte accounting overflow"};
            }
            _messages.push_back(std::move(envelope));
            _charged_bytes += charge;
        }

        [[nodiscard]] ActorEnvelope pop()
        {
            if (_messages.empty())
            {
                throw std::logic_error{"Mailbox underflow"};
            }
            ActorEnvelope env = std::move(_messages.front());
            _messages.pop_front();
            _charged_bytes -= env.chargedBytes();
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

    enum class SyntheticAwaitOutcome : std::uint8_t
    {
        Completed = 0,
        Rejected = 1,
        TimedOut = 2,
        Cancelled = 3,
    };

    struct SyntheticAwait
    {
    };

    enum class ActorTaskStatus : std::uint8_t
    {
        Suspended = 0,
        Completed = 1,
    };

    class ActorTask final
    {
    public:
        struct promise_type;
        using Handle = std::coroutine_handle<promise_type>;

        struct promise_type
        {
            std::variant<std::monostate, CompletedTurn, std::exception_ptr> result{};
            std::optional<SyntheticAwaitOutcome> await_outcome{std::nullopt};

            ActorTask get_return_object() noexcept;

            std::suspend_always initial_suspend() noexcept
            {
                return {};
            }

            std::suspend_always final_suspend() noexcept
            {
                return {};
            }

            void unhandled_exception() noexcept
            {
                result = std::current_exception();
            }

            void return_value(CompletedTurn completed) noexcept
            {
                result = std::move(completed);
            }

            auto await_transform(SyntheticAwait) noexcept
            {
                struct Awaiter
                {
                    promise_type& promise;

                    bool await_ready() const noexcept
                    {
                        return false;
                    }

                    void await_suspend(std::coroutine_handle<promise_type>) noexcept
                    {
                    }

                    SyntheticAwaitOutcome await_resume()
                    {
                        if (std::holds_alternative<std::exception_ptr>(promise.result))
                        {
                            std::rethrow_exception(std::get<std::exception_ptr>(promise.result));
                        }
                        if (!promise.await_outcome.has_value())
                        {
                            throw std::logic_error{"ActorTask resumed without await outcome"};
                        }
                        const auto outcome = *promise.await_outcome;
                        promise.await_outcome.reset();
                        return outcome;
                    }
                };
                return Awaiter{*this};
            }
        };

        ActorTask() noexcept = default;

        explicit ActorTask(Handle handle) noexcept
            : _handle(handle)
        {
        }

        ~ActorTask()
        {
            if (_handle)
            {
                _handle.destroy();
                _handle = nullptr;
            }
        }

        ActorTask(const ActorTask&) = delete;
        ActorTask& operator=(const ActorTask&) = delete;

        ActorTask(ActorTask&& other) noexcept
            : _handle(std::exchange(other._handle, nullptr))
        {
        }

        ActorTask& operator=(ActorTask&& other) noexcept
        {
            if (this != &other)
            {
                if (_handle)
                {
                    _handle.destroy();
                }
                _handle = std::exchange(other._handle, nullptr);
            }
            return *this;
        }

        [[nodiscard]] bool valid() const noexcept
        {
            return _handle != nullptr;
        }

        [[nodiscard]] bool done() const noexcept
        {
            return _handle != nullptr && _handle.done();
        }

        [[nodiscard]] ActorTaskStatus resume()
        {
            if (!_handle || _handle.done())
            {
                throw std::logic_error{"Cannot resume invalid or finished ActorTask"};
            }

            _handle.resume();

            auto& promise = _handle.promise();
            if (std::holds_alternative<std::exception_ptr>(promise.result))
            {
                std::rethrow_exception(std::get<std::exception_ptr>(promise.result));
            }

            return _handle.done() ? ActorTaskStatus::Completed : ActorTaskStatus::Suspended;
        }

        [[nodiscard]] ActorTaskStatus resume(const SyntheticAwaitOutcome outcome)
        {
            if (!_handle || _handle.done())
            {
                throw std::logic_error{"Cannot resume invalid or finished ActorTask"};
            }

            _handle.promise().await_outcome = outcome;
            _handle.resume();

            auto& promise = _handle.promise();
            if (std::holds_alternative<std::exception_ptr>(promise.result))
            {
                std::rethrow_exception(std::get<std::exception_ptr>(promise.result));
            }

            return _handle.done() ? ActorTaskStatus::Completed : ActorTaskStatus::Suspended;
        }

        [[nodiscard]] CompletedTurn takeCompleted()
        {
            if (!_handle || !_handle.done())
            {
                throw std::logic_error{"ActorTask is not completed"};
            }

            auto& promise = _handle.promise();
            if (std::holds_alternative<std::exception_ptr>(promise.result))
            {
                std::rethrow_exception(std::get<std::exception_ptr>(promise.result));
            }

            if (!std::holds_alternative<CompletedTurn>(promise.result))
            {
                throw std::logic_error{"ActorTask completed without return value"};
            }

            CompletedTurn completed = std::move(std::get<CompletedTurn>(promise.result));
            promise.result = std::monostate{};
            return completed;
        }

    private:
        Handle _handle{nullptr};
    };

    inline ActorTask ActorTask::promise_type::get_return_object() noexcept
    {
        return ActorTask{Handle::from_promise(*this)};
    }

    struct SuspendedTurn
    {
        ActorTask task;
    };

    using TurnResult = std::variant<CompletedTurn, SuspendedTurn>;

    struct SyntheticSuspendedCommand
    {
        AwaitKey key;
        TimePoint deadline;
        ActorTask task;
        std::optional<SyntheticAwaitOutcome> completion{std::nullopt};
    };

    struct ActivationLoad
    {
        AwaitKey key;
        TimePoint deadline;
    };

    enum class SyntheticActivationOutcome : std::uint8_t
    {
        Ready,
        Rejected,
        TimedOut,
        Cancelled,
    };

    using BlockedTask = std::variant<SyntheticSuspendedCommand, ActivationLoad>;

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
        std::chrono::milliseconds await_timeout{2000};
        std::size_t max_concurrent_loading{1024};
    };

    [[nodiscard]] inline bool isValid(const WorkerActorConfig& config) noexcept
    {
        return config.actor_table_capacity > 0 &&
               config.max_mailbox_messages_per_actor > 0 &&
               config.max_mailbox_bytes_per_actor > 0 &&
               config.max_mailbox_messages_total >= config.max_mailbox_messages_per_actor &&
               config.max_mailbox_bytes_total >= config.max_mailbox_bytes_per_actor &&
               config.max_turns_per_actor_slice > 0 &&
               config.worker_shutdown_timeout >= std::chrono::milliseconds::zero() &&
               config.await_timeout > std::chrono::milliseconds::zero() &&
               config.max_concurrent_loading > 0;
    }

    struct WorkerActorMetrics
    {
        std::uint64_t actor_turns{0};
        std::uint64_t suspended_turns{0};
        std::uint64_t resumed_turns{0};
        std::uint64_t stale_completions{0};
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
        std::uint64_t activation_loads_started{0};
        std::uint64_t activation_load_failures{0};
        std::uint64_t loading_limit_rejections{0};
        std::uint64_t stale_activation_completions{0};
        std::uint64_t stale_await_timeouts{0};
        std::uint64_t cancelled_blocked_actors{0};
        std::uint64_t forced_blocked_destructions{0};
        std::uint64_t remote_tells_sent{0};
        std::uint64_t remote_tell_rejections{0};
        std::uint64_t remote_tells_received{0};
        std::uint64_t remote_tells_delivered{0};
        std::uint64_t remote_tell_delivery_failures{0};
        std::uint64_t misrouted_actor_events{0};
        std::uint64_t actor_events_without_runtime{0};
        std::uint64_t total_slice_duration_ns{0};
        std::chrono::nanoseconds max_slice_duration{0};
    };
}
