#pragma once

#include "snf/runtime/actor_key.hpp"

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace snf::runtime
{
    enum class AsyncOperationOutcome : std::uint8_t
    {
        Pending,
        Completed,
        Cancelled,
    };

    enum class ContinuationRejection : std::uint8_t
    {
        DuplicateCompletion,
        AlreadyCancelled,
    };

    struct ActorContinuation
    {
        ActorKey target;
        ActorIncarnation incarnation;
        TaskId task;
    };

    class ContinuationEndpoint
    {
    public:
        virtual ~ContinuationEndpoint() = default;

        ContinuationEndpoint(const ContinuationEndpoint&) = delete;
        ContinuationEndpoint& operator=(const ContinuationEndpoint&) = delete;

        [[nodiscard]] virtual bool publish(const ActorContinuation& continuation) noexcept = 0;

        virtual void reportRejectedCompletion(const ActorContinuation& continuation, ContinuationRejection rejection) noexcept = 0;

    protected:
        ContinuationEndpoint() = default;
    };

    struct ActorCompletionHandle
    {
        std::shared_ptr<ContinuationEndpoint> endpoint;
        ActorContinuation continuation;
    };

    class AsyncOperationControl
    {
    public:
        virtual ~AsyncOperationControl() = default;

        AsyncOperationControl(const AsyncOperationControl&) = delete;
        AsyncOperationControl& operator=(const AsyncOperationControl&) = delete;

        void requestCancel() noexcept
        {
            _cancel_requested.store(true, std::memory_order_release);
        }

        [[nodiscard]] bool isCancelRequested() const noexcept
        {
            return _cancel_requested.load(std::memory_order_acquire);
        }

        [[nodiscard]] bool claimCancelled() noexcept
        {
            AsyncOperationOutcome expected = AsyncOperationOutcome::Pending;
            return _outcome.compare_exchange_strong(expected, AsyncOperationOutcome::Cancelled, std::memory_order_acq_rel, std::memory_order_acquire);
        }

        [[nodiscard]] AsyncOperationOutcome outcome() const noexcept
        {
            return _outcome.load(std::memory_order_acquire);
        }

    protected:
        AsyncOperationControl() = default;

        [[nodiscard]] AsyncOperationOutcome claimCompleted() noexcept
        {
            AsyncOperationOutcome expected = AsyncOperationOutcome::Pending;
            if (_outcome.compare_exchange_strong(expected, AsyncOperationOutcome::Completed, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                return AsyncOperationOutcome::Pending;
            }

            return expected;
        }

    private:
        std::atomic<AsyncOperationOutcome> _outcome{AsyncOperationOutcome::Pending};
        std::atomic<bool> _cancel_requested{false};
    };

    class AsyncOperationRejected final : public std::runtime_error
    {
    public:
        AsyncOperationRejected()
            : std::runtime_error{"Actor async operation was rejected before it started"}
        {
        }
    };

    class AsyncOperationCancelled final : public std::runtime_error
    {
    public:
        AsyncOperationCancelled()
            : std::runtime_error{"Actor async operation was cancelled"}
        {
        }
    };

    template <typename T> class AsyncOperationState final : public AsyncOperationControl
    {
        static_assert(std::is_nothrow_move_constructible_v<T>,
                      "An async operation result must move without throwing: the producer stores "
                      "it inside a noexcept window that a waiting Worker depends on.");

    public:
        AsyncOperationState() = default;

        void complete(const ActorCompletionHandle& handle, T result) noexcept
        {
            publishTerminal(handle, [this, &result]() noexcept { _value.emplace(std::move(result)); });
        }

        void fail(const ActorCompletionHandle& handle, std::exception_ptr error) noexcept
        {
            publishTerminal(handle, [this, &error]() noexcept { _error = std::move(error); });
        }

        [[nodiscard]] T takeResult()
        {
            if (_error)
            {
                std::rethrow_exception(_error);
            }

            if (!_value)
            {
                throw std::logic_error{"Async operation completed without a result"};
            }

            return std::move(*_value);
        }

    private:
        template <typename StoreResult> void publishTerminal(const ActorCompletionHandle& handle, StoreResult store) noexcept
        {
            const AsyncOperationOutcome lost_to = claimCompleted();
            if (lost_to != AsyncOperationOutcome::Pending)
            {
                if (handle.endpoint)
                {
                    handle.endpoint->reportRejectedCompletion(handle.continuation,
                                                              lost_to == AsyncOperationOutcome::Completed ? ContinuationRejection::DuplicateCompletion : ContinuationRejection::AlreadyCancelled);
                }
                return;
            }

            store();

            if (handle.endpoint)
            {
                static_cast<void>(handle.endpoint->publish(handle.continuation));
            }
        }

        std::optional<T> _value;
        std::exception_ptr _error;
    };

    template <typename T> class AsyncOperationProducer final
    {
    public:
        AsyncOperationProducer(std::shared_ptr<AsyncOperationState<T>> state, ActorCompletionHandle handle) noexcept
            : _state(std::move(state))
            , _handle(std::move(handle))
        {
        }

        void complete(T result) noexcept
        {
            if (_state)
            {
                _state->complete(_handle, std::move(result));
            }
        }

        void fail(std::exception_ptr error) noexcept
        {
            if (_state)
            {
                _state->fail(_handle, std::move(error));
            }
        }

    private:
        std::shared_ptr<AsyncOperationState<T>> _state;
        ActorCompletionHandle _handle;
    };
}
