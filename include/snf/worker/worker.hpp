#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/connection_table.hpp"
#include "snf/worker/connection_work_queue.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poll_registration.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/request_sink.hpp"
#include "snf/worker/timer_queue.hpp"
#include "snf/worker/wakeup.hpp"
#include "snf/worker/worker_network.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <vector>

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

        WorkerNetworkMetrics network{};
    };

    class Worker final
    {
    public:
        using TimePoint = std::chrono::steady_clock::time_point;
        using EventHandler = std::function<void(WorkerEvent&&)>;
        using TimerHandler = std::function<void(TimerPayload&&)>;

        Worker(WorkerId id, std::uint16_t worker_count, WorkerBudgets budgets, WorkerInboxConfig inbox_config);
        Worker(
            WorkerId id,
            std::uint16_t worker_count,
            WorkerBudgets budgets,
            WorkerInboxConfig inbox_config,
            WorkerNetworkConfig network_config,
            RequestSink& request_sink
        );

        Worker(const Worker&) = delete;
        Worker& operator=(const Worker&) = delete;

        void run();                  // 호출 thread를 owner로 고정
        void requestStop() noexcept; // 다른 thread에서 호출 가능

        // Network startup is deliberately separate from the generic worker
        // skeleton so actor/runtime users can opt into it incrementally.
        void configureNetwork(const WorkerNetworkConfig& config, RequestSink& request_sink);
        void attachListener(snf::net::UniqueFileDescriptor listener);
        void bindRemoteTarget(WorkerId target, WorkerInboxPort port);

        // owner Worker thread 전용. The rvalue Frame is consumed by this
        // call, including terminal failures; no result payload is returned.
        // A remote target is converted to a RemoteConnectionSend in the
        // target Worker's inbox.
        [[nodiscard]] SendResult send(ConnectionRef connection, snf::protocol::Frame&& frame, bool critical = false);
        [[nodiscard]] bool closeConnection(ConnectionRef connection, CloseReason reason, bool graceful = true);

        [[nodiscard]] bool networkEnabled() const noexcept;
        [[nodiscard]] std::size_t connectionCount() const noexcept;
        [[nodiscard]] bool listenerPaused() const noexcept;

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
        void processReadQueue(const IoBudget& budget, TimePoint phase_started_at);
        void acceptPendingClients(const IoBudget& budget, TimePoint phase_started_at, std::size_t& accepted);
        [[nodiscard]] bool readConnection(
            ConnectionHandle handle,
            const IoBudget& budget,
            TimePoint phase_started_at,
            std::uint64_t& bytes_read,
            std::size_t& frames_decoded,
            bool& budget_exhausted
        );
        void drainInbox(const InboxBudget& budget);
        void expireTimers(TimePoint now, const CountTimeBudget& budget);
        void runReadyActors(const CountTimeBudget& budget); // 4단계까지 no-op
        void flushWrites(const ByteTimeBudget& budget);     // 3단계까지 no-op

        void onEvent(WorkerEvent&& event);
        void onTimer(TimerPayload&& payload);

        [[nodiscard]] SendResult sendLocal(ConnectionRef connection, snf::protocol::Frame&& frame, bool critical);
        [[nodiscard]] bool enqueueRead(ConnectionSlot& slot);
        [[nodiscard]] bool enqueueWrite(ConnectionSlot& slot);
        void updateConnectionInterest(const ConnectionSlot& slot);
        void forceClose(ConnectionHandle handle, CloseReason reason);
        [[nodiscard]] bool beginGracefulClose(ConnectionHandle handle, CloseReason reason);
        void maybeResumeListener();
        void pauseListener();
        [[noreturn]] void networkInvariantViolation(const char* message);
        void beginNetworkShutdown();
        void runNetworkShutdown();
        void releaseConnectionRegistration(ConnectionHandle handle) noexcept;
        [[nodiscard]] std::optional<PollRegistrationView> registrationFor(ConnectionHandle handle) const noexcept;
        [[nodiscard]] bool isCurrent(ConnectionHandle handle) const noexcept;

        WorkerId _id;
        std::uint16_t _worker_count;
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

        std::unique_ptr<ConnectionTable> _connections;
        std::unique_ptr<PollRegistrationTable> _registrations;
        std::unique_ptr<ReadWorkQueue> _read_work_queue;
        std::unique_ptr<WriteWorkQueue> _write_work_queue;
        std::vector<std::optional<PollRegistrationHandle>> _connection_registrations;
        std::vector<WorkerInboxPort> _remote_ports;
        snf::net::UniqueFileDescriptor _listener;
        std::optional<PollRegistrationHandle> _listener_registration;
        RequestSink* _request_sink{nullptr};
        NullRequestSink _null_request_sink;
        WorkerNetworkConfig _network_config{};
        bool _listener_paused{false};
        bool _network_stopping{false};
    };
}
