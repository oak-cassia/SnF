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

    // The two ways a completion is thrown away are separate facts, so they are
    // counted separately. A duplicate means the producer published twice; a
    // cancelled one means the owning Worker had already given up on the operation.
    enum class ContinuationRejection : std::uint8_t
    {
        DuplicateCompletion,
        AlreadyCancelled,
    };

    // Identifies which suspended task a completion belongs to. The Worker checks
    // both the incarnation and the task id, so a completion for an activation
    // that has since been evicted cannot resume the actor that replaced it.
    struct ActorContinuation
    {
        ActorKey target;
        ActorIncarnation incarnation;
        TaskId task;
    };

    // Routes a completion to the owning Worker's continuation queue.
    //
    // Ref-counted on purpose: the coroutine contract allows a blocking operation
    // to outlive a cancel, so a producer must not hold a raw pointer to a runtime
    // that may already be gone.
    //
    // Deactivation is only safe once every Worker has joined and no operation is
    // in flight. Turning it off any earlier would strand a Worker that lost the
    // cancel race and is waiting for an already-claimed completion.
    class ContinuationEndpoint
    {
    public:
        virtual ~ContinuationEndpoint() = default;

        ContinuationEndpoint(const ContinuationEndpoint&) = delete;
        ContinuationEndpoint& operator=(const ContinuationEndpoint&) = delete;

        // false means the endpoint is inactive and the completion was discarded.
        // A reserved slot guarantees this never fails for capacity reasons.
        [[nodiscard]] virtual bool publish(const ActorContinuation& continuation) noexcept = 0;

        virtual void reportRejectedCompletion(const ActorContinuation& continuation,
                                              ContinuationRejection rejection) noexcept = 0;

    protected:
        ContinuationEndpoint() = default;
    };

    // Everything a producer needs to reach the owning Worker, as a value. It
    // holds no Actor, no ActorSlot and no coroutine handle.
    struct ActorCompletionHandle
    {
        std::shared_ptr<ContinuationEndpoint> endpoint;
        ActorContinuation continuation;
    };

    // The type-erased half of an operation state. An actor slot holds this so it
    // can cancel an operation without knowing the result type.
    class AsyncOperationControl
    {
    public:
        virtual ~AsyncOperationControl() = default;

        AsyncOperationControl(const AsyncOperationControl&) = delete;
        AsyncOperationControl& operator=(const AsyncOperationControl&) = delete;

        // Callable from any thread. It only raises a flag and never decides the
        // terminal outcome: the owning Worker performs the Pending -> Cancelled
        // transition, so a coroutine is never resumed from a foreign thread.
        void requestCancel() noexcept
        {
            _cancel_requested.store(true, std::memory_order_release);
        }

        [[nodiscard]] bool isCancelRequested() const noexcept
        {
            return _cancel_requested.load(std::memory_order_acquire);
        }

        // Owning Worker only. Winning this claim guarantees that no producer will
        // ever write a result, so the awaiter can be resumed with a cancellation.
        // Losing it means a completion is already claimed and its publish is
        // guaranteed to arrive, so the Worker must wait for it instead.
        [[nodiscard]] bool claimCancelled() noexcept
        {
            AsyncOperationOutcome expected = AsyncOperationOutcome::Pending;
            return _outcome.compare_exchange_strong(expected,
                                                    AsyncOperationOutcome::Cancelled,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire);
        }

        [[nodiscard]] AsyncOperationOutcome outcome() const noexcept
        {
            return _outcome.load(std::memory_order_acquire);
        }

    protected:
        AsyncOperationControl() = default;

        // Producer side. Claiming before writing the result is what keeps a
        // cancelling Worker and a completing producer from touching the result
        // slot at the same time.
        [[nodiscard]] AsyncOperationOutcome claimCompleted() noexcept
        {
            AsyncOperationOutcome expected = AsyncOperationOutcome::Pending;
            if (_outcome.compare_exchange_strong(expected,
                                                 AsyncOperationOutcome::Completed,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire))
            {
                return AsyncOperationOutcome::Pending;
            }

            return expected;
        }

    private:
        std::atomic<AsyncOperationOutcome> _outcome{AsyncOperationOutcome::Pending};
        std::atomic<bool> _cancel_requested{false};
    };

    // Thrown into the handler when the operation could not be started because the
    // Worker's in-flight reservation was exhausted. It is raised before the
    // operation begins, so nothing external needs undoing.
    class AsyncOperationRejected final : public std::runtime_error
    {
    public:
        AsyncOperationRejected()
            : std::runtime_error{"Actor async operation was rejected before it started"}
        {
        }
    };

    // Thrown into the handler when the owning Worker cancelled the operation.
    // Unwinding the coroutine is what lets the handler run its own cleanup.
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

        // Producer side, and the only place a result is written.
        //
        // Everything slow -- the work itself -- happens before this call. Between
        // the claim and the publish there is no blocking, no allocation and no
        // throwing, because a Worker that lost the cancel race is waiting for
        // exactly this publish and has no other way to finish.
        void complete(const ActorCompletionHandle& handle, T result) noexcept
        {
            publishTerminal(handle,
                            [this, &result]() noexcept { _value.emplace(std::move(result)); });
        }

        void fail(const ActorCompletionHandle& handle, std::exception_ptr error) noexcept
        {
            publishTerminal(handle, [this, &error]() noexcept { _error = std::move(error); });
        }

        // Owning Worker only, after the continuation for this operation has been
        // consumed. A producer failure is rethrown into the awaiting handler.
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
        template <typename StoreResult>
        void publishTerminal(const ActorCompletionHandle& handle, StoreResult store) noexcept
        {
            const AsyncOperationOutcome lost_to = claimCompleted();
            if (lost_to != AsyncOperationOutcome::Pending)
            {
                if (handle.endpoint)
                {
                    handle.endpoint->reportRejectedCompletion(
                        handle.continuation,
                        lost_to == AsyncOperationOutcome::Completed
                            ? ContinuationRejection::DuplicateCompletion
                            : ContinuationRejection::AlreadyCancelled);
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

    // Handed to the external service. Bundling the state with the completion
    // handle keeps the producer's API to complete() and fail(), with no way to
    // publish without having claimed the terminal transition first.
    template <typename T> class AsyncOperationProducer final
    {
    public:
        AsyncOperationProducer(std::shared_ptr<AsyncOperationState<T>> state,
                               ActorCompletionHandle handle) noexcept
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
