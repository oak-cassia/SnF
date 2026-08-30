#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/identity.hpp"
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
        std::uint64_t max_queued_bytes{1024ULL * 1024};
        std::chrono::milliseconds operation_timeout{2000};
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

    using DbRequest = std::variant<LoadPlayerRequest>;

    struct LoadedPlayerRow
    {
        std::uint64_t player_id{0};
        std::uint64_t handled_command_count{0};
        bool has_location{false};
        std::uint64_t zone_id{0};
        std::uint32_t position_x{0};
        std::uint32_t position_y{0};
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

    using DbResult = std::variant<LoadPlayerResult, DbFailure>;

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

        void expireDeadlines(DbTimePoint now);

        // Reopens slots whose reconnect backoff has elapsed. Driven from the worker
        // loop, never from a timer of its own.
        void maintainConnections(DbTimePoint now);

        // Stops accepting new work. In-flight completions still arrive.
        void beginShutdown() noexcept;
        [[nodiscard]] bool shuttingDown() const noexcept;

        // Owner thread only. Closes handles and releases the thread-local driver
        // state. Safe to call more than once.
        void shutdown(Poller& poller);

        [[nodiscard]] const DbClientMetrics& metrics() const noexcept;

    private:
        struct Impl;
        std::unique_ptr<Impl> _impl;
    };
}
