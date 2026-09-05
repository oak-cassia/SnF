#pragma once

#include "snf/worker/actor.hpp"
#include "snf/worker/actor_table.hpp"
#include "snf/worker/barrier.hpp"
#include "snf/worker/budget.hpp"
#include "snf/worker/connection_table.hpp"
#include "snf/worker/connection_work_queue.hpp"
#include "snf/worker/db_client.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poll_registration.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/progress.hpp"
#include "snf/worker/request_sink.hpp"
#include "snf/worker/timer_queue.hpp"
#include "snf/worker/wakeup.hpp"
#include "snf/worker/worker_network.hpp"

#include <array>
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

    struct WorkerGaugeSnapshot
    {
        std::size_t connections{0};
        std::size_t actors{0};
        std::size_t loading{0};
        std::size_t ready_actors{0};
        std::size_t mailbox_messages_total{0};
        std::uint64_t mailbox_bytes_total{0};
        std::size_t timer_entries{0};
        std::uint64_t application_timer_bytes{0};
        // Refreshed every 64 loop iterations and at every shutdown phase
        // boundary, unlike every other field here, which is exact for the sample.
        std::uint64_t sampled_inbox_queued_bytes{0};
        std::size_t db_queued_operations{0};
        std::uint64_t db_queued_bytes{0};
        std::size_t db_in_flight{0};
    };

    struct WorkerHighWaterMarks
    {
        // Updated at the mutation site, so transient peaks are not lost.
        std::size_t connection_read_buffer_bytes{0};
        std::size_t connection_write_queued_bytes{0};
        std::size_t db_queued_operations{0};
        std::uint64_t db_queued_bytes{0};
        std::size_t db_in_flight{0};

        // Sampled once per loop iteration. A value that rises and falls inside one
        // phase can therefore be missed; admission tests remain the capacity proof.
        std::size_t sampled_connections{0};
        std::size_t sampled_actors{0};
        std::size_t sampled_loading{0};
        std::size_t sampled_ready_actors{0};
        std::size_t sampled_mailbox_messages_total{0};
        std::uint64_t sampled_mailbox_bytes_total{0};
        std::size_t sampled_timer_entries{0};
        std::uint64_t sampled_application_timer_bytes{0};

        // Cross-thread lane counters are sampled every 64 loop iterations.
        std::uint64_t sampled_inbox_queued_bytes{0};
    };

    struct WorkerPhaseMetrics
    {
        std::uint64_t entries{0};
        std::uint64_t budget_stops{0};
        // Wall time includes scheduler descheduling. CPU time and voluntary
        // context switches distinguish actual owner-thread work/blocking from
        // involuntary preemption; wall time remains the fairness/stall bound.
        std::chrono::nanoseconds max_residence{0};
        std::chrono::nanoseconds max_cpu_residence{0};
        std::chrono::nanoseconds max_entry_gap{0};
        std::uint64_t voluntary_context_switches{0};
        std::uint64_t involuntary_context_switches{0};
        // Attribution for the same sample that established max_residence.
        std::chrono::nanoseconds max_wall_residence_cpu{0};
        std::uint64_t max_wall_residence_voluntary_context_switches{0};
        std::uint64_t max_wall_residence_involuntary_context_switches{0};
        // Owner-thread bookkeeping. Reports should use the durations above.
        std::chrono::steady_clock::time_point last_entered{};
    };

    struct WorkerResourceSnapshot
    {
        std::size_t connections{0};
        std::size_t actors{0};
        std::size_t blocked_actors{0};
        std::size_t loading{0};
        std::uint64_t inbox_bytes{0};
        std::size_t timer_entries{0};
        std::uint64_t application_timer_bytes{0};
        std::size_t db_in_flight{0};
        std::size_t db_queued{0};
    };

    struct WorkerShutdownPhaseRecord
    {
        bool entered{false};
        std::chrono::nanoseconds duration{0};
        bool deadline_hit{false};
        WorkerResourceSnapshot remaining{};
    };

    struct WorkerShutdownMetrics
    {
        std::chrono::nanoseconds actor_configured_timeout{0};
        std::chrono::nanoseconds actor_phases_duration{0};
        std::chrono::nanoseconds db_configured_timeout{0};
        std::chrono::nanoseconds db_shutdown_duration{0};
        std::chrono::nanoseconds total_duration{0};
        bool actor_deadline_exceeded{false};
        bool db_deadline_exceeded{false};
        WorkerShutdownPhaseRecord phase_a{};
        WorkerShutdownPhaseRecord phase_b{};
        WorkerShutdownPhaseRecord phase_c{};
        WorkerShutdownPhaseRecord phase_d{};
        WorkerResourceSnapshot final_resources{};
        std::uint64_t forced_connection_closes{0};
        std::uint64_t forced_actor_removals{0};
        std::uint64_t forced_ready_queue_drops{0};
    };

    struct WorkerMetrics
    {
        std::uint64_t loop_iterations{0};
        std::uint64_t poll_events{0};
        // Number of budget-stop events, not iterations: one poll phase can stop on
        // the event count and then again on the shared phase duration.
        std::uint64_t poll_budget_stops{0};
        std::uint64_t wakeups_consumed{0};
        std::uint64_t inbox_events{0};
        std::uint64_t inbox_budget_stops{0};
        std::uint64_t timers_fired{0};
        std::uint64_t timer_budget_stops{0};
        std::uint64_t thread_execution_sample_failures{0};

        // 셧다운 drain 루프가 처리한 양은 별도로 센다.
        // 이렇게 나누지 않으면 "메인 루프가 일했다"는 사실을 테스트가 증명할 수 없다.
        std::uint64_t shutdown_inbox_events{0};
        std::uint64_t shutdown_timers_fired{0};
        std::uint64_t shutdown_quiescence_rounds{0};
        std::uint64_t shutdown_barrier_timeouts{0};
        std::uint64_t shutdown_barrier_aborts{0};

        WorkerNetworkMetrics network{};
        WorkerActorMetrics actor{};
        WorkerGaugeSnapshot gauges{};
        WorkerHighWaterMarks high_water_marks{};
        std::array<WorkerPhaseMetrics, WORKER_PHASE_COUNT> phases{};
        LatencyHistogram loop_iteration_ns{};
        WorkerShutdownMetrics shutdown{};
    };

    class Worker final : public TimerAdmission, public DbCompletionSink
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

        // The MYSQL handles are not created here: mysql_init() performs the
        // per-thread driver initialisation, so the client only starts connecting
        // once run() has bound the owner thread.
        void configureDb(const DbClientConfig& config);
        [[nodiscard]] bool dbEnabled() const noexcept;
        [[nodiscard]] const DbClientMetrics& dbMetrics() const noexcept;

        // DbCompletionSink. Owner thread only, and never resumes a coroutine inline.
        void completeDb(AwaitKey key, DbResult result) override;

        // owner Worker thread 전용. The rvalue Frame is consumed by this
        // call, including terminal failures; no result payload is returned.
        // A remote target is converted to a RemoteConnectionSend in the
        // target Worker's inbox.
        [[nodiscard]] SendResult send(ConnectionRef connection, snf::protocol::Frame&& frame, bool critical = false);
        [[nodiscard]] bool closeConnection(ConnectionRef connection, CloseReason reason, bool graceful = true);

        // Send an actor message. If local, delivers to local mailbox; if remote, routes to target WorkerInbox.
        // Owner thread only.
        [[nodiscard]] DeliveryResult tell(ActorKey key, ActorEnvelope envelope);

        // Notify actor about connection closure with receipt callback routing.
        // If local, delivers directly to local mailbox (or emits ActorAbsent receipt if absent).
        // Remote Accepted means inbox admission only, not a receipt. MailboxAccepted
        // acknowledges mailbox admission, not actor execution or cleanup completion.
        // ActorAbsent does not establish that domain occupancy has been cleaned up.
        // Local receipt callbacks can run before this method returns: callers must
        // install pending state before calling and tolerate synchronous removal.
        // Owner thread only. Retries may enqueue duplicate notifications.
        [[nodiscard]] DeliveryResult notifyActorConnectionClosed(ActorKey key, ConnectionRef connection, ActorEnvelope envelope);

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

        // progress() is the only Worker observation API that is safe from any
        // thread while run() is active. It does not publish any other Worker data.
        [[nodiscard]] const WorkerProgress& progress() const noexcept;

        // Immutable after startup. WorkerGroup uses the sum of the actor/
        // connection budget and the optional DB budget as its join deadline base.
        [[nodiscard]] std::chrono::nanoseconds configuredShutdownTimeout() const noexcept;

        [[nodiscard]] WorkerId id() const noexcept;

    private:
        struct ThreadExecutionSample
        {
            std::chrono::nanoseconds cpu_time{0};
            std::uint64_t voluntary_context_switches{0};
            std::uint64_t involuntary_context_switches{0};
        };

        class ActorTurnScope final
        {
        public:
            explicit ActorTurnScope(Worker& worker) noexcept;
            ~ActorTurnScope() noexcept;

            ActorTurnScope(const ActorTurnScope&) = delete;
            ActorTurnScope& operator=(const ActorTurnScope&) = delete;

            [[nodiscard]] std::uint64_t id() const noexcept;

        private:
            Worker& _worker;
            std::uint64_t _id{0};
        };

        void startDb();
        void advanceDb(TimePoint now);
        void sampleGauges() noexcept;
        [[nodiscard]] static bool sampleThreadExecution(ThreadExecutionSample& sample) noexcept;
        void enterPhase(WorkerPhase phase, TimePoint now) noexcept;
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
        void onTimer(TimerPayload&& payload, TimePoint now);

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
        [[nodiscard]] DeliveryResult deliverActorConnectionClosedLocally(ActorKey key, ConnectionRef connection, ActorEnvelope envelope);
        void sendActorConnectionClosedReceipt(ActorKey key, ConnectionRef connection, ActorConnectionClosedResult result);
        void applyEffectBatch(ActorSlot& current_slot, EffectBatch&& batch, bool& stopped, std::uint64_t turn_id);
        // Completion source for the Step 5 synthetic scaffold. The source owns the stale metric
        // decision so tryMarkSyntheticCommandReady() stays metric-free and 5C's timer path can
        // raise stale_await_timeouts instead. Step 8's completeDb() takes over this role.
        bool completeSyntheticCommand(AwaitKey key, SyntheticAwaitOutcome outcome);
        [[nodiscard]] bool tryMarkSyntheticCommandReady(AwaitKey key, SyntheticAwaitOutcome outcome);
        [[nodiscard]] bool tryMarkDbCommandReady(AwaitKey key, DbResult result);
        [[nodiscard]] bool suspendOnDbRequest(ActorSlot& slot, ActorTask task);
        [[nodiscard]] DeliveryResult beginActivationLoad(ActorKey key, ActorEnvelope&& first_message);
        void completeSyntheticActivation(AwaitKey key, SyntheticActivationOutcome outcome);
        void removeActor(ActorHandle handle, ActorRemovalReason reason);
        MailboxUsage discardMailbox(ActorSlot& slot);
        [[nodiscard]] bool hasBlockedActors() const noexcept;
        [[nodiscard]] std::size_t blockedActorCount() const noexcept;

        friend struct WorkerActorTestAccess;

        void runUnifiedShutdown();
        void beginShutdownPhaseA();
        [[nodiscard]] bool cancelBlockedActorsForShutdown();
        void runShutdownPhaseB(TimePoint deadline);
        void runShutdownPhaseC(TimePoint deadline);
        void runShutdownPhaseD(TimePoint deadline);
        void captureShutdownPhase(WorkerShutdownPhaseRecord& record, TimePoint entered_at, TimePoint deadline) noexcept;
        [[nodiscard]] WorkerResourceSnapshot captureResourceSnapshot() const noexcept;

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
        WorkerMetrics _metrics{};   // owner thread 전용 plain counter
        WorkerProgress _progress{}; // cross-thread 관측 가능한 packed atomic
        WorkerPhase _current_phase{WorkerPhase::Starting};
        TimePoint _phase_entered_at{};
        ThreadExecutionSample _phase_execution_entered_at{};
        bool _phase_execution_sample_valid{false};
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
        std::unique_ptr<DbClient> _db;
        bool _db_started{false};
        std::chrono::milliseconds _db_shutdown_timeout{0};
        ActorFactory* _actor_factory{nullptr};
        WorkerActorConfig _actor_config{};
        OperationIdSource _operation_ids{};
        std::uint64_t _last_actor_turn_id{0};
        std::optional<std::uint64_t> _active_actor_turn_id{std::nullopt};
        std::size_t _total_mailbox_messages{0};
        std::uint64_t _total_mailbox_bytes{0};
        std::size_t _loading_count{0};
        bool _shutting_down{false};
    };
}
