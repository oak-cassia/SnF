#pragma once

#include "snf/worker/actor.hpp"
#include "snf/worker/actor_table.hpp"
#include "snf/worker/barrier.hpp"
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
    struct WorkerActorTestAccess;

    enum class ActorRemovalReason : std::uint8_t
    {
        Stopped,
        ActivationRejected,
        ActivationTimedOut,
        ActivationCancelled,
        ShutdownForced,
    };

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
        std::uint64_t shutdown_quiescence_rounds{0};
        std::uint64_t shutdown_barrier_timeouts{0};
        std::uint64_t shutdown_barrier_aborts{0};

        WorkerNetworkMetrics network{};
        WorkerActorMetrics actor{};
    };

    class Worker final : public TimerAdmission
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
        Worker(
            WorkerId id,
            std::uint16_t worker_count,
            WorkerBudgets budgets,
            WorkerInboxConfig inbox_config,
            WorkerActorConfig actor_config,
            ActorFactory& actor_factory
        );
        Worker(
            WorkerId id,
            std::uint16_t worker_count,
            WorkerBudgets budgets,
            WorkerInboxConfig inbox_config,
            WorkerNetworkConfig network_config,
            RequestSink& request_sink,
            WorkerActorConfig actor_config,
            ActorFactory& actor_factory
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
        void attachBarrier(WorkerQuiescenceBarrier* barrier) noexcept;

        void configureActors(const WorkerActorConfig& config, ActorFactory& factory);

        // owner Worker thread 전용. The rvalue Frame is consumed by this
        // call, including terminal failures; no result payload is returned.
        // A remote target is converted to a RemoteConnectionSend in the
        // target Worker's inbox.
        [[nodiscard]] SendResult send(ConnectionRef connection, snf::protocol::Frame&& frame, bool critical = false);
        [[nodiscard]] bool closeConnection(ConnectionRef connection, CloseReason reason, bool graceful = true);

        // Send an actor message. If local, delivers to local mailbox; if remote, routes to target WorkerInbox.
        // Owner thread only.
        [[nodiscard]] DeliveryResult tell(ActorKey key, ActorEnvelope envelope);

        // Transitional local implementation primitive used by RequestSink and TellActorEffect.
        // Owner thread only.
        [[nodiscard]] DeliveryResult tryDeliverLocal(ActorKey key, ActorEnvelope envelope);

        [[nodiscard]] bool networkEnabled() const noexcept;
        [[nodiscard]] std::size_t connectionCount() const noexcept;
        [[nodiscard]] bool listenerPaused() const noexcept;

        [[nodiscard]] bool actorsConfigured() const noexcept;
        [[nodiscard]] std::size_t actorCount() const noexcept;
        [[nodiscard]] std::size_t loadingCount() const noexcept;
        [[nodiscard]] std::size_t totalMailboxMessages() const noexcept;
        [[nodiscard]] std::uint64_t totalMailboxBytes() const noexcept;

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
        void runReadyActors(const CountTimeBudget& budget);
        void flushWrites(const ByteTimeBudget& budget);

        void onEvent(WorkerEvent&& event);
        void onTimer(TimerPayload&& payload);

        [[nodiscard]] SendResult sendLocal(ConnectionRef connection, snf::protocol::Frame&& frame, bool critical);
        [[nodiscard]] bool enqueueRead(ConnectionSlot& slot);
        [[nodiscard]] bool enqueueWrite(ConnectionSlot& slot);
        void updateConnectionInterest(const ConnectionSlot& slot);
        void forceClose(ConnectionHandle handle, CloseReason reason);
        [[nodiscard]] bool beginGracefulClose(ConnectionHandle handle, CloseReason reason, std::optional<TimePoint> max_deadline = std::nullopt);
        void maybeResumeListener();
        void pauseListener();
        [[noreturn]] void networkInvariantViolation(const char* message);
        void releaseConnectionRegistration(ConnectionHandle handle) noexcept;
        [[nodiscard]] std::optional<PollRegistrationView> registrationFor(ConnectionHandle handle) const noexcept;
        [[nodiscard]] bool isCurrent(ConnectionHandle handle) const noexcept;

        [[nodiscard]] std::optional<TimerReservation> tryReserve(std::uint64_t charged_bytes, std::uint64_t turn_id) noexcept override;
        void releaseReservation(std::uint64_t charged_bytes) noexcept override;

        [[nodiscard]] DeliveryResult tellInternal(ActorKey key, ActorEnvelope envelope, bool allow_quiescing);
        [[nodiscard]] DeliveryResult tryDeliverLocalInternal(ActorKey key, ActorEnvelope envelope);
        void applyEffectBatch(ActorSlot& current_slot, EffectBatch&& batch, bool& stopped);
        // Completion source for the Step 5 synthetic scaffold. The source owns the stale metric
        // decision so tryMarkSyntheticCommandReady() stays metric-free and 5C's timer path can
        // raise stale_await_timeouts instead. Step 8's completeDb() takes over this role.
        bool completeSyntheticCommand(AwaitKey key, SyntheticAwaitOutcome outcome);
        [[nodiscard]] bool tryMarkSyntheticCommandReady(AwaitKey key, SyntheticAwaitOutcome outcome);
        [[nodiscard]] DeliveryResult beginActivationLoad(ActorKey key, ActorEnvelope&& first_message);
        void completeSyntheticActivation(AwaitKey key, SyntheticActivationOutcome outcome);
        void removeActor(ActorHandle handle, ActorRemovalReason reason);
        MailboxUsage discardMailbox(ActorSlot& slot);
        [[nodiscard]] bool hasBlockedActors() const noexcept;

        friend struct WorkerActorTestAccess;

        void runUnifiedShutdown();
        void beginShutdownPhaseA();
        void runShutdownPhaseB(TimePoint deadline);
        void runShutdownPhaseC(TimePoint deadline);
        void runShutdownPhaseD(TimePoint deadline);

        WorkerId _id;
        std::uint16_t _worker_count;
        WorkerBudgets _budgets;
        WakeupHandle _wakeup;
        Poller _poller;
        WorkerInbox _inbox;
        TimerQueue _timers;
        WorkerQuiescenceBarrier* _barrier{nullptr};
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

        std::unique_ptr<ActorTable> _actors;
        std::unique_ptr<ReadyActorQueue> _ready_queue;
        ActorFactory* _actor_factory{nullptr};
        WorkerActorConfig _actor_config{};
        OperationIdSource _operation_ids{};
        std::size_t _total_mailbox_messages{0};
        std::uint64_t _total_mailbox_bytes{0};
        std::size_t _loading_count{0};
        bool _shutting_down{false};
    };
}
