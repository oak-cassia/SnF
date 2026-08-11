#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace snf::runtime
{
    enum class ActorTaskStatus
    {
        Suspended,
        Completed,
    };

    template <typename T> class ActorTask;

    namespace detail
    {
        template <typename T> class ActorTaskPromise final
        {
        public:
            [[nodiscard]] ActorTask<T> get_return_object() noexcept;

            // Lazy: the scheduler owns the first resume, so a handler body never
            // runs on the thread that merely created the task.
            [[nodiscard]] std::suspend_always initial_suspend() const noexcept
            {
                return {};
            }

            // The frame stays alive at completion so the owning Worker can read
            // the result and then destroy it explicitly.
            [[nodiscard]] std::suspend_always final_suspend() const noexcept
            {
                return {};
            }

            template <typename U> void return_value(U&& result)
            {
                _value.emplace(std::forward<U>(result));
            }

            void unhandled_exception() noexcept
            {
                _error = std::current_exception();
            }

        private:
            std::optional<T> _value;
            std::exception_ptr _error;

            friend class ActorTask<T>;
        };
    }

    // A single actor command's execution. The frame is owned by the owning
    // Worker's actor slot: only that Worker resumes it and only that Worker
    // destroys it, which is what keeps an external completion from ever touching
    // a coroutine handle.
    template <typename T> class ActorTask final
    {
    public:
        using promise_type = detail::ActorTaskPromise<T>;

        ActorTask() noexcept = default;

        ActorTask(const ActorTask&) = delete;
        ActorTask& operator=(const ActorTask&) = delete;

        ActorTask(ActorTask&& other) noexcept
            : _handle(std::exchange(other._handle, {}))
        {
        }

        ActorTask& operator=(ActorTask&& other) noexcept
        {
            if (this != &other)
            {
                destroy();
                _handle = std::exchange(other._handle, {});
            }

            return *this;
        }

        ~ActorTask()
        {
            destroy();
        }

        [[nodiscard]] bool valid() const noexcept
        {
            return static_cast<bool>(_handle);
        }

        [[nodiscard]] ActorTaskStatus resume()
        {
            _handle.resume();
            return _handle.done() ? ActorTaskStatus::Completed : ActorTaskStatus::Suspended;
        }

        // Valid only once resume() has reported Completed. A handler exception is
        // rethrown here so it reaches the scheduler's existing worker-failure
        // path instead of being swallowed by the coroutine frame.
        [[nodiscard]] T takeResult()
        {
            promise_type& promise = _handle.promise();
            if (promise._error)
            {
                std::rethrow_exception(promise._error);
            }

            return std::move(*promise._value);
        }

    private:
        explicit ActorTask(const std::coroutine_handle<promise_type> handle) noexcept
            : _handle(handle)
        {
        }

        void destroy() noexcept
        {
            if (_handle)
            {
                _handle.destroy();
                _handle = {};
            }
        }

        std::coroutine_handle<promise_type> _handle{};

        friend class detail::ActorTaskPromise<T>;
    };

    namespace detail
    {
        template <typename T> ActorTask<T> ActorTaskPromise<T>::get_return_object() noexcept
        {
            return ActorTask<T>{std::coroutine_handle<ActorTaskPromise<T>>::from_promise(*this)};
        }
    }
}
