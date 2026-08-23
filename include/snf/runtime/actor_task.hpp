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

            [[nodiscard]] std::suspend_always initial_suspend() const noexcept
            {
                return {};
            }

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
