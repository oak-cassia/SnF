#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/latency_histogram.hpp"
#include "snf/worker/poll_token.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace snf::worker
{
    class Poller;

    using DbTimePoint = std::chrono::steady_clock::time_point;

    // Stage 8A measured the TLS handshake holding the worker thread for ~8ms per
    // connect while an unencrypted connect costs ~100us, and the client library
    // defaults to PREFERRED. Naming the mode is therefore mandatory rather than
    // optional: the default must never be inherited silently.
    enum class DbSslMode : std::uint8_t
    {
        Disabled = 0,
        Preferred = 1,
        Required = 2,
    };

    struct DbConnectionId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const DbConnectionId&) const noexcept = default;
    };

    // Separate from the poll token's 32-bit generation: the token filters stale
    // epoll events cheaply, this settles them. A reconnect can reuse the same file
    // descriptor, so nothing may progress on a generation mismatch.
    struct DbConnectionGeneration
    {
        std::uint64_t value{0};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const DbConnectionGeneration&) const noexcept = default;
    };

    struct DbClientConfig
    {
        // IP only. Both drivers resolve hostnames synchronously inside connect, so
        // resolution happens once at startup, off the worker loop.
        std::string host_ip;
        std::uint16_t port{3306};
        std::string user;
        std::string password;
        std::string database;
        DbSslMode ssl_mode{DbSslMode::Disabled};

        std::size_t connection_count{2};
        std::size_t max_queued_operations{256};
        // Sum of sizeof(DbRequest) and every queued request's owned dynamic
        // allocation. SavePlayerRequest skill storage is charged by capacity(), not
        // size(), because this is a heap-memory bound rather than a logical payload
        // bound. Deque node overhead is fixed by max_queued_operations.
        std::uint64_t max_queued_bytes{1024ULL * 1024};
        std::chrono::milliseconds operation_timeout{2000};
        // Bounds the asynchronous connect/query/fetch/cancel progress attempted
        // during Worker shutdown. Final driver teardown happens after this budget.
        std::chrono::milliseconds shutdown_timeout{1000};
        // A refused connect returns immediately, so reopening a slot without a
        // delay would spin the whole worker while the database is down.
        std::chrono::milliseconds reconnect_backoff{250};

        // Enforced while streaming rather than measured afterwards.
        std::size_t max_result_rows{4096};
        std::uint64_t max_result_bytes{4ULL * 1024 * 1024};
    };

    [[nodiscard]] bool isValid(const DbClientConfig& config) noexcept;

    // Requests are a closed set with fixed row shapes, not arbitrary SQL. The result
    // bound comes from the shape; a query whose maximum row count and row width are
    // not known does not belong here.
    //
    // These carry plain scalars instead of game types so that snf_worker keeps its
    // current dependencies. The adapter layer maps PlayerRecord to and from them.
    struct LoadPlayerRequest
    {
        std::uint64_t player_id{0};
    };

    // One logical operation covering the whole transaction, exactly as the blocking
    // repository does it: the player row upsert and the full replacement of the
    // skill set commit together or not at all. Splitting this into separate requests
    // would let the loadout drift from the row it belongs to.
    struct SavePlayerRequest
    {
        std::uint64_t player_id{0};
        std::uint64_t handled_command_count{0};
        bool has_location{false};
        std::uint64_t zone_id{0};
        std::int32_t position_x{0};
        std::int32_t position_y{0};
        std::uint64_t currency_balance{0};
        std::uint64_t purchased_item_count{0};
        std::uint64_t street_experience{0};
        std::uint32_t equipped_skill_id{0};
        // Must be non-empty, matching the existing repository's rejection of an
        // empty loadout.
        std::vector<std::uint32_t> owned_skill_ids{};
    };

    using DbRequest = std::variant<LoadPlayerRequest, SavePlayerRequest>;

    struct LoadedPlayerRow
    {
        std::uint64_t player_id{0};
        std::uint64_t handled_command_count{0};
        bool has_location{false};
        std::uint64_t zone_id{0};
        // Signed, matching the INT columns and ZonePosition. Parsing these as
        // unsigned would reject a negative coordinate the legacy path accepts.
        std::int32_t position_x{0};
        std::int32_t position_y{0};
        std::uint64_t currency_balance{0};
        std::uint64_t purchased_item_count{0};
        std::uint64_t street_experience{0};
        std::uint32_t equipped_skill_id{0};
    };

    struct LoadPlayerResult
    {
        bool found{false};
        LoadedPlayerRow row{};
        std::vector<std::uint32_t> owned_skill_ids{};
    };

    enum class DbFailureKind : std::uint8_t
    {
        // Admission refused the request; nothing was sent.
        Overloaded = 0,
        // The logical deadline passed. Whether anything reached the server depends
        // on where the operation was: see DbFailure::reached_server.
        TimedOut = 1,
        // The connection died or was poisoned under an in-flight operation.
        ConnectionLost = 2,
        // The server answered with an error, or a row failed to decode.
        QueryFailed = 3,
        // A row-count or byte cap tripped mid-stream.
        ResultTooLarge = 4,
    };

    struct DbFailure
    {
        DbFailureKind kind{DbFailureKind::Overloaded};
        // False for a request that never left the queue. A queued timeout leaves the
        // connection untouched; only an in-flight one poisons it.
        bool reached_server{false};
        std::string message{};
    };

    // A COMMIT whose answer never arrived is its own outcome. Folding it into
    // failure would invite a retry that duplicates a mutation the server already
    // applied.
    enum class SaveOutcome : std::uint8_t
    {
        // The server acknowledged the COMMIT.
        Committed = 0,
        // The transaction failed before COMMIT was dispatched, and the uncommitted
        // work is known to be gone: either ROLLBACK was acknowledged, or the
        // connection was dropped, which discards it.
        FailedBeforeCommit = 1,
        // The COMMIT driver call was started and no acknowledgement came back. The
        // transaction may or may not have been applied. Never retried automatically.
        CommitOutcomeUnknown = 2,
    };

    struct SavePlayerResult
    {
        SaveOutcome outcome{SaveOutcome::FailedBeforeCommit};
    };

    using DbResult = std::variant<LoadPlayerResult, SavePlayerResult, DbFailure>;

    enum class DbSubmitStatus : std::uint8_t
    {
        // No blocked state was created; the caller reports overload immediately.
        Rejected = 0,
        // Finished inside the call; no blocked state was created.
        CompletedInline = 1,
        // Accepted. The caller confirms its blocked state and waits for completeDb().
        Pending = 2,
    };

    struct DbSubmitResult
    {
        DbSubmitStatus status{DbSubmitStatus::Rejected};
        // Only set for CompletedInline.
        std::optional<DbResult> inline_result{std::nullopt};
    };

    // Implemented by Worker, the same way TimerAdmission is. Completion never
    // resumes a coroutine inline; it records the result and makes the actor runnable.
    class DbCompletionSink
    {
    public:
        virtual ~DbCompletionSink() = default;
        virtual void completeDb(AwaitKey key, DbResult result) = 0;
    };

    struct DbClientMetrics
    {
        std::uint64_t operations_started{0};
        std::uint64_t operations_completed{0};
        std::uint64_t operations_failed{0};
        std::uint64_t queued_timeouts{0};
        std::uint64_t in_flight_timeouts{0};
        std::uint64_t connections_opened{0};
        // A slot only leaves Connecting when a readiness event routed through the
        // worker's poller wakes it, so this counter also proves that path works.
        std::uint64_t connections_ready{0};
        std::uint64_t connections_poisoned{0};
        std::uint64_t stale_poll_events{0};
        std::uint64_t submit_rejections{0};
        std::uint64_t budget_yields{0};
        std::uint64_t commits_acknowledged{0};
        std::uint64_t commits_unknown{0};
        std::uint64_t rollbacks{0};
        std::size_t queued_operations_high_water{0};
        std::uint64_t queued_bytes_high_water{0};
        std::size_t in_flight_high_water{0};
        LatencyHistogram operation_latency_ns{};
        LatencyHistogram queue_wait_ns{};
    };

    // Worker-local. Every MYSQL handle is created, used and destroyed on the owner
    // thread, so the client owns wire state only and never an actor pointer or a
    // coroutine handle.
    class DbClient final
    {
    public:
        DbClient(DbClientConfig config, DbCompletionSink& sink);
        ~DbClient();

        DbClient(const DbClient&) = delete;
        DbClient& operator=(const DbClient&) = delete;

        // Owner thread only. Creates the MYSQL handles and begins connecting. Call
        // after the worker thread has started, never from the thread that built the
        // config: mysql_init() performs the per-thread initialisation.
        void start(Poller& poller);

        [[nodiscard]] DbSubmitResult tryStart(AwaitKey key, DbRequest request, DbTimePoint deadline);

        // Two-step stale check: the token's index and 32-bit generation filter, then
        // the slot's 64-bit generation decides.
        void onPollEvent(PollToken token);

        // Progresses in-flight work under a bound. Returns true when work remains
        // that needs no further readiness event, in which case the worker must not
        // sleep in epoll_wait.
        [[nodiscard]] bool advance(const DbProgressBudget& budget);

        [[nodiscard]] bool hasLocalWork() const noexcept;
        [[nodiscard]] std::size_t inFlightCount() const noexcept;
        [[nodiscard]] std::size_t queuedCount() const noexcept;
        [[nodiscard]] std::uint64_t queuedBytes() const noexcept;

        void expireDeadlines(DbTimePoint now);

        // Reopens slots whose reconnect backoff has elapsed. Driven from the worker
        // loop, never from a timer of its own.
        void maintainConnections(DbTimePoint now);

        // Stops accepting new work. In-flight completions still arrive.
        void beginShutdown() noexcept;
        [[nodiscard]] bool shuttingDown() const noexcept;

        // Owner thread only. Async progress stops at deadline; handles are then
        // force-closed and the final driver teardown runs outside that budget.
        // Returns true when live async state had to be forced at the deadline.
        // Safe to call more than once.
        bool shutdown(Poller& poller, DbTimePoint deadline);

        [[nodiscard]] const DbClientMetrics& metrics() const noexcept;

    private:
        struct Impl;
        std::unique_ptr<Impl> _impl;
    };
}
