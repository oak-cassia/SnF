#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/timer_queue.hpp"
#include "snf/worker/wakeup.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <span>
#include <thread>

namespace snf::worker
{
    struct WorkerMetrics
    {
        std::uint64_t loop_iterations{0};
        std::uint64_t poll_events{0};
        std::uint64_t wakeups_consumed{0};
        std::uint64_t inbox_events{0};
        std::uint64_t inbox_budget_stops{0};
        std::uint64_t timers_fired{0};
        std::uint64_t timer_budget_stops{0};

        // 셧다운 drain 루프가 처리한 양은 별도로 센다.
        // 이렇게 나누지 않으면 "메인 루프가 일했다"는 사실을 테스트가 증명할 수 없다.
        std::uint64_t shutdown_inbox_events{0};
        std::uint64_t shutdown_timers_fired{0};
    };

    class Worker final
    {
    public:
        using TimePoint = std::chrono::steady_clock::time_point;
        using EventHandler = std::function<void(WorkerEvent&&)>;
        using TimerHandler = std::function<void(TimerPayload&&)>;

        Worker(WorkerId id, std::uint16_t worker_count, WorkerBudgets budgets, WorkerInboxConfig inbox_config);

        Worker(const Worker&) = delete;
        Worker& operator=(const Worker&) = delete;

        void run();                  // 호출 thread를 owner로 고정
        void requestStop() noexcept; // 다른 thread에서 호출 가능

        // startup 전용 — Worker thread 시작 전에만 호출한다.
        [[nodiscard]] WorkerInboxPort bindInboxSource(WorkerId source) noexcept;

        // startup 전용 — Worker thread 시작 전에만 설치한다.
        // 4단계에서 Actor dispatch가, 8단계에서 completion 처리가 이 자리를 대체한다.
        void setEventHandler(EventHandler handler);
        void setTimerHandler(TimerHandler handler);

        // owner thread 전용
        [[nodiscard]] bool trySchedule(TimePoint deadline, TimerPayload payload);

        // metrics() 계약:
        // - Worker owner thread에서 호출 가능
        // - 또는 run()이 반환하고 thread를 join한 뒤 호출 가능
        // - 실행 중 외부 thread 호출 금지 (TSan data race 방지)
        [[nodiscard]] const WorkerMetrics& metrics() const noexcept;

        [[nodiscard]] WorkerId id() const noexcept;

    private:
        void bindOwnerThread() noexcept;
        void assertOwnerThread() const noexcept;
        [[nodiscard]] bool hasRunnableWork() const noexcept;
        [[nodiscard]] std::optional<std::chrono::milliseconds> pollTimeout() const;

        void processPollEvents(std::span<const PollEvent> events, const IoBudget& budget);
        void drainInbox(const InboxBudget& budget);
        void expireTimers(TimePoint now, const CountTimeBudget& budget);
        void runReadyActors(const CountTimeBudget& budget); // 4단계까지 no-op
        void flushWrites(const ByteTimeBudget& budget);     // 3단계까지 no-op

        void onEvent(WorkerEvent&& event);
        void onTimer(TimerPayload&& payload);

        WorkerId _id;
        WorkerBudgets _budgets;
        WakeupHandle _wakeup;
        Poller _poller;
        WorkerInbox _inbox;
        TimerQueue _timers;
        std::atomic<bool> _stop_requested{false};
        std::thread::id _owner_thread{};
        WorkerMetrics _metrics{};    // owner thread 전용 plain counter
        bool _inbox_has_more{false}; // hasRunnableWork() 계산용
        bool _timers_have_due{false};
        EventHandler _event_handler{};
        TimerHandler _timer_handler{};
    };
}
