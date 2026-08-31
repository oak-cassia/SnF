#include "snf/worker/db_client.hpp"

#include "snf/worker/poller.hpp"

#include <mysql/mysql.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace
{
    using Clock = std::chrono::steady_clock;

    // The row shapes are fixed, so the result bound is a property of the query
    // rather than something measured after the fact. LIMIT bounds the row count;
    // the column list bounds the row width, and every column here is a fixed-width
    // integer. A query with a variable-length column may not be added without
    // registering its maximum length too.
    constexpr std::size_t PLAYER_ROW_COLUMNS = 8;
    constexpr std::size_t MAX_PLAYER_ROWS = 2; // 2 so a duplicate identity is detectable
    constexpr std::size_t MAX_PLAYER_SKILL_ROWS = 64;
    constexpr std::uint64_t MAX_PLAYER_ROW_BYTES = 256;

    static_assert(MAX_PLAYER_SKILL_ROWS * MAX_PLAYER_ROW_BYTES < 4ULL * 1024 * 1024);

    [[nodiscard]] bool budgetExpired(const Clock::time_point started_at, const std::chrono::nanoseconds limit) noexcept
    {
        return Clock::now() - started_at >= limit;
    }

    [[nodiscard]] bool parseUnsigned(const char* const text, const unsigned long length, std::uint64_t& out) noexcept
    {
        if (text == nullptr)
        {
            return false;
        }
        const auto [end, error] = std::from_chars(text, text + length, out);
        return error == std::errc{} && end == text + length;
    }

    [[nodiscard]] bool parseSigned(const char* const text, const unsigned long length, std::int64_t& out) noexcept
    {
        if (text == nullptr)
        {
            return false;
        }
        const auto [end, error] = std::from_chars(text, text + length, out);
        return error == std::errc{} && end == text + length;
    }

    [[nodiscard]] unsigned int sslModeValue(const snf::worker::DbSslMode mode) noexcept
    {
        switch (mode)
        {
        case snf::worker::DbSslMode::Disabled:
            return SSL_MODE_DISABLED;
        case snf::worker::DbSslMode::Preferred:
            return SSL_MODE_PREFERRED;
        case snf::worker::DbSslMode::Required:
            return SSL_MODE_REQUIRED;
        }
        return SSL_MODE_DISABLED;
    }

    [[nodiscard]] std::string playerSelect(const std::uint64_t player_id)
    {
        return "SELECT handled_command_count, zone_id, position_x, position_y, "
               "currency_balance, purchased_item_count, street_experience, equipped_skill_id "
               "FROM snf_players WHERE player_id=" +
               std::to_string(player_id) + " LIMIT " + std::to_string(MAX_PLAYER_ROWS);
    }

    [[nodiscard]] std::string playerUpsert(const snf::worker::SavePlayerRequest& request)
    {
        const std::string zone = request.has_location ? std::to_string(request.zone_id) : "NULL";
        const std::string position_x = request.has_location ? std::to_string(request.position_x) : "NULL";
        const std::string position_y = request.has_location ? std::to_string(request.position_y) : "NULL";
        return "INSERT INTO snf_players (player_id, handled_command_count, zone_id, "
               "position_x, position_y, currency_balance, purchased_item_count, street_experience, equipped_skill_id) VALUES (" +
               std::to_string(request.player_id) + "," + std::to_string(request.handled_command_count) + "," + zone + "," + position_x + "," +
               position_y + "," + std::to_string(request.currency_balance) + "," + std::to_string(request.purchased_item_count) + "," +
               std::to_string(request.street_experience) + "," + std::to_string(request.equipped_skill_id) +
               ") ON DUPLICATE KEY UPDATE "
               "handled_command_count=VALUES(handled_command_count), "
               "zone_id=VALUES(zone_id), position_x=VALUES(position_x), "
               "position_y=VALUES(position_y), currency_balance=VALUES(currency_balance), "
               "purchased_item_count=VALUES(purchased_item_count), "
               "street_experience=VALUES(street_experience), equipped_skill_id=VALUES(equipped_skill_id)";
    }

    [[nodiscard]] std::string playerSkillDelete(const std::uint64_t player_id)
    {
        return "DELETE FROM snf_player_skills WHERE player_id=" + std::to_string(player_id);
    }

    [[nodiscard]] std::string playerSkillInsert(const snf::worker::SavePlayerRequest& request)
    {
        std::string sql = "INSERT INTO snf_player_skills (player_id, skill_id) VALUES ";
        for (std::size_t index = 0; index < request.owned_skill_ids.size(); ++index)
        {
            if (index != 0)
            {
                sql += ',';
            }
            sql += '(' + std::to_string(request.player_id) + ',' + std::to_string(request.owned_skill_ids[index]) + ')';
        }
        return sql;
    }

    [[nodiscard]] std::string playerSkillSelect(const std::uint64_t player_id)
    {
        return "SELECT skill_id FROM snf_player_skills WHERE player_id=" + std::to_string(player_id) + " ORDER BY skill_id LIMIT " +
               std::to_string(MAX_PLAYER_SKILL_ROWS);
    }
}

namespace snf::worker
{
    bool isValid(const DbClientConfig& config) noexcept
    {
        if (config.host_ip.empty() || config.user.empty() || config.database.empty())
        {
            return false;
        }
        // Hostnames resolve synchronously inside connect, so they are rejected here
        // rather than silently costing the worker a DNS lookup.
        if (config.host_ip.find_first_not_of("0123456789.:abcdefABCDEF") != std::string::npos)
        {
            return false;
        }
        return config.port != 0 && config.connection_count > 0 && config.max_queued_operations > 0 && config.max_queued_bytes > 0 &&
               config.operation_timeout > std::chrono::milliseconds::zero() && config.reconnect_backoff >= std::chrono::milliseconds::zero() &&
               config.max_result_rows > 0 && config.max_result_bytes > 0;
    }

    struct DbClient::Impl
    {
        enum class SlotState : std::uint8_t
        {
            Closed,
            Connecting,
            Idle,
            Querying,
            Fetching,
            FreeingResult,
        };

        enum class Stage : std::uint8_t
        {
            PlayerRow,
            PlayerSkills,
            // The save transaction, in the order the blocking repository runs it.
            SaveIsolation,
            SaveBegin,
            SaveUpsert,
            SaveDeleteSkills,
            SaveInsertSkills,
            SaveCommit,
            SaveRollback,
        };

        struct InFlight
        {
            AwaitKey key;
            DbRequest request;
            DbTimePoint deadline;
            Stage stage{Stage::PlayerRow};
            LoadPlayerResult result{};
            std::size_t rows{0};
            std::uint64_t bytes{0};
            // Set the moment the COMMIT driver call is started. From here on the
            // outcome cannot be narrowed to success or failure by observation.
            bool commit_dispatched{false};
        };

        struct Queued
        {
            AwaitKey key;
            DbRequest request;
            DbTimePoint deadline;
        };

        struct Slot
        {
            DbConnectionId id{};
            DbConnectionGeneration generation{};
            MYSQL* handle{nullptr};
            MYSQL_RES* result{nullptr};
            SlotState state{SlotState::Closed};
            int registered_fd{-1};
            PollInterest interest{};
            bool runnable{false};
            DbTimePoint retry_at{};
            std::optional<InFlight> in_flight{std::nullopt};
        };

        Impl(DbClientConfig configuration, DbCompletionSink& completion_sink)
            : config(std::move(configuration))
            , sink(completion_sink)
        {
            slots.resize(config.connection_count);
            for (std::size_t index = 0; index < slots.size(); ++index)
            {
                slots[index].id = DbConnectionId{static_cast<std::uint32_t>(index)};
            }
        }

        DbClientConfig config;
        DbCompletionSink& sink;
        std::vector<Slot> slots;
        std::deque<Queued> queue;
        Poller* poller{nullptr};
        std::uint64_t next_generation{1};
        bool shutting_down{false};
        DbClientMetrics metrics{};

        [[nodiscard]] PollToken tokenFor(const Slot& slot) const noexcept
        {
            return PollToken{
                .kind = PollTargetKind::DbConnection,
                .index = slot.id.value,
                .generation = static_cast<std::uint32_t>(slot.generation.value & 0xFFFFFFFFULL),
            };
        }

        void openSlot(Slot& slot)
        {
            assert(slot.handle == nullptr);
            slot.generation = DbConnectionGeneration{next_generation++};
            slot.handle = ::mysql_init(nullptr);
            if (slot.handle == nullptr)
            {
                throw std::runtime_error{"mysql_init failed"};
            }

            unsigned int ssl_mode = sslModeValue(config.ssl_mode);
            static_cast<void>(::mysql_options(slot.handle, MYSQL_OPT_SSL_MODE, &ssl_mode));
            if (config.ssl_mode == DbSslMode::Disabled)
            {
                // caching_sha2_password needs the server's public key for a full
                // authentication on an unencrypted link. Without this the first
                // connect for a user the server has not cached would fail.
                bool get_public_key = true;
                static_cast<void>(::mysql_options(slot.handle, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_public_key));
            }

            slot.state = SlotState::Connecting;
            slot.runnable = true;
            ++metrics.connections_opened;
        }

        void closeSlot(Slot& slot)
        {
            if (slot.result != nullptr)
            {
                ::mysql_free_result(slot.result);
                slot.result = nullptr;
            }
            if (slot.registered_fd != -1 && poller != nullptr)
            {
                poller->remove(slot.registered_fd);
                slot.registered_fd = -1;
                slot.interest = PollInterest{};
            }
            if (slot.handle != nullptr)
            {
                ::mysql_close(slot.handle);
                slot.handle = nullptr;
            }
            slot.state = SlotState::Closed;
            slot.runnable = false;
        }

        // A connection whose protocol stream was cut mid-operation cannot be reused:
        // with mysql_use_result() an undrained result set makes the next query fail
        // with "commands out of sync". Bumping the generation is what makes any
        // late epoll event for the old descriptor stale.
        void poisonSlot(Slot& slot, DbFailure failure)
        {
            ++metrics.connections_poisoned;
            std::optional<InFlight> victim = std::move(slot.in_flight);
            slot.in_flight.reset();
            closeSlot(slot);

            if (victim.has_value())
            {
                ++metrics.operations_failed;
                if (std::holds_alternative<SavePlayerRequest>(victim->request) && victim->commit_dispatched)
                {
                    ++metrics.commits_unknown;
                }
                sink.completeDb(victim->key, failureResultFor(*victim, std::move(failure)));
            }

            // Reopening goes through the same backoff gate as a failed connect so
            // that a dead database cannot turn poisoning into a hot loop.
            slot.retry_at = Clock::now();
        }

        void syncInterest(Slot& slot)
        {
            if (poller == nullptr || slot.handle == nullptr)
            {
                return;
            }
            const int descriptor = slot.handle->net.fd;
            if (descriptor == -1)
            {
                return;
            }

            // The driver publishes no wait direction once a call has parked
            // (net.reading_or_writing reads 0), so readability is the default and
            // writability is added only while it reports a write in progress.
            const bool writing = slot.handle->net.reading_or_writing == 2;
            const PollInterest interest{.read = !writing, .write = writing};

            if (slot.registered_fd != descriptor)
            {
                if (slot.registered_fd != -1)
                {
                    poller->remove(slot.registered_fd);
                }
                poller->add(descriptor, tokenFor(slot), interest);
                slot.registered_fd = descriptor;
                slot.interest = interest;
                return;
            }
            if (interest != slot.interest)
            {
                poller->modify(descriptor, tokenFor(slot), interest);
                slot.interest = interest;
            }
        }

        void finish(Slot& slot, DbResult result)
        {
            assert(slot.in_flight.has_value());
            const AwaitKey key = slot.in_flight->key;
            slot.in_flight.reset();
            slot.state = SlotState::Idle;
            slot.runnable = true; // may be able to pick up queued work without a poll
            ++metrics.operations_completed;
            sink.completeDb(key, std::move(result));
        }

        void failOperation(Slot& slot, const DbFailureKind kind, std::string message)
        {
            poisonSlot(
                slot,
                DbFailure{
                    .kind = kind,
                    .reached_server = true,
                    .message = std::move(message),
                }
            );
        }

        [[nodiscard]] std::string currentSql(const InFlight& in_flight) const
        {
            if (const auto* load = std::get_if<LoadPlayerRequest>(&in_flight.request))
            {
                return in_flight.stage == Stage::PlayerRow ? playerSelect(load->player_id) : playerSkillSelect(load->player_id);
            }

            const auto& save = std::get<SavePlayerRequest>(in_flight.request);
            switch (in_flight.stage)
            {
            case Stage::SaveIsolation:
                return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
            case Stage::SaveBegin:
                return "START TRANSACTION";
            case Stage::SaveUpsert:
                return playerUpsert(save);
            case Stage::SaveDeleteSkills:
                return playerSkillDelete(save.player_id);
            case Stage::SaveInsertSkills:
                return playerSkillInsert(save);
            case Stage::SaveCommit:
                return "COMMIT";
            case Stage::SaveRollback:
                return "ROLLBACK";
            default:
                break;
            }
            return "ROLLBACK";
        }

        [[nodiscard]] std::size_t stageRowLimit(const InFlight& in_flight) const noexcept
        {
            return in_flight.stage == Stage::PlayerRow ? MAX_PLAYER_ROWS : MAX_PLAYER_SKILL_ROWS;
        }

        [[nodiscard]] static bool isSaveStage(const Stage stage) noexcept
        {
            return stage >= Stage::SaveIsolation;
        }

        // The result a failure produces depends on how far the transaction got.
        [[nodiscard]] static DbResult failureResultFor(const InFlight& in_flight, DbFailure failure)
        {
            if (!std::holds_alternative<SavePlayerRequest>(in_flight.request))
            {
                return DbResult{std::move(failure)};
            }
            // Dropping the connection discards an uncommitted transaction, so
            // anything before COMMIT was dispatched is known not to have applied.
            return DbResult{SavePlayerResult{
                .outcome = in_flight.commit_dispatched ? SaveOutcome::CommitOutcomeUnknown : SaveOutcome::FailedBeforeCommit,
            }};
        }

        void beginStageQuery(Slot& slot)
        {
            assert(slot.in_flight.has_value());
            slot.state = SlotState::Querying;
            slot.runnable = true;
        }

        void assign(Slot& slot, Queued&& queued)
        {
            const bool is_save = std::holds_alternative<SavePlayerRequest>(queued.request);
            slot.in_flight = InFlight{
                .key = queued.key,
                .request = std::move(queued.request),
                .deadline = queued.deadline,
                .stage = is_save ? Stage::SaveIsolation : Stage::PlayerRow,
            };
            ++metrics.operations_started;
            beginStageQuery(slot);
        }

        void pumpQueue()
        {
            for (Slot& slot : slots)
            {
                if (queue.empty())
                {
                    return;
                }
                if (slot.state != SlotState::Idle || slot.in_flight.has_value())
                {
                    continue;
                }
                Queued queued = std::move(queue.front());
                queue.pop_front();
                assign(slot, std::move(queued));
            }
        }

        // One driver step. Returns true when the slot made progress and may be able
        // to continue without waiting for the socket.
        [[nodiscard]] bool step(Slot& slot, std::size_t& rows_used, std::uint64_t& bytes_used)
        {
            switch (slot.state)
            {
            case SlotState::Closed:
                slot.runnable = false;
                return false;

            case SlotState::Connecting:
            {
                const net_async_status status = ::mysql_real_connect_nonblocking(
                    slot.handle,
                    config.host_ip.c_str(),
                    config.user.c_str(),
                    config.password.c_str(),
                    config.database.c_str(),
                    config.port,
                    nullptr,
                    0
                );
                if (status == NET_ASYNC_NOT_READY)
                {
                    syncInterest(slot);
                    slot.runnable = false;
                    return false;
                }
                if (status == NET_ASYNC_ERROR)
                {
                    // Nothing is in flight while connecting, so no operation fails
                    // here. A refused connect returns immediately, so the retry has
                    // to wait: reopening straight away would spin the worker for as
                    // long as the database is down.
                    closeSlot(slot);
                    slot.retry_at = Clock::now() + config.reconnect_backoff;
                    slot.runnable = false;
                    return false;
                }
                syncInterest(slot);
                slot.state = SlotState::Idle;
                slot.runnable = true;
                ++metrics.connections_ready;
                return true;
            }

            case SlotState::Idle:
                if (slot.in_flight.has_value())
                {
                    beginStageQuery(slot);
                    return true;
                }
                slot.runnable = false;
                return false;

            case SlotState::Querying:
            {
                InFlight& in_flight = *slot.in_flight;
                // Set before the call, not after: once the driver has been asked to
                // send COMMIT there is no way to prove it did not reach the server.
                if (in_flight.stage == Stage::SaveCommit)
                {
                    in_flight.commit_dispatched = true;
                }

                const std::string sql = currentSql(in_flight);
                const net_async_status status = ::mysql_real_query_nonblocking(slot.handle, sql.c_str(), sql.size());
                if (status == NET_ASYNC_NOT_READY)
                {
                    syncInterest(slot);
                    slot.runnable = false;
                    return false;
                }
                if (status == NET_ASYNC_ERROR)
                {
                    if (isSaveStage(in_flight.stage))
                    {
                        onSaveStatementFailed(slot, ::mysql_error(slot.handle));
                        return false;
                    }
                    failOperation(slot, DbFailureKind::QueryFailed, ::mysql_error(slot.handle));
                    return false;
                }

                // A statement with no result set has nothing to stream.
                if (::mysql_field_count(slot.handle) == 0)
                {
                    return advanceStage(slot);
                }

                // Streaming retrieval: rows are checked against the caps as they
                // arrive instead of buffering the whole set first.
                slot.result = ::mysql_use_result(slot.handle);
                if (slot.result == nullptr)
                {
                    failOperation(slot, DbFailureKind::QueryFailed, ::mysql_error(slot.handle));
                    return false;
                }
                slot.state = SlotState::Fetching;
                slot.runnable = true;
                return true;
            }

            case SlotState::Fetching:
            {
                MYSQL_ROW row = nullptr;
                const net_async_status status = ::mysql_fetch_row_nonblocking(slot.result, &row);
                if (status == NET_ASYNC_NOT_READY)
                {
                    syncInterest(slot);
                    slot.runnable = false;
                    return false;
                }
                if (status == NET_ASYNC_ERROR)
                {
                    failOperation(slot, DbFailureKind::QueryFailed, ::mysql_error(slot.handle));
                    return false;
                }
                if (row == nullptr)
                {
                    slot.state = SlotState::FreeingResult;
                    slot.runnable = true;
                    return true;
                }

                ++rows_used;
                InFlight& in_flight = *slot.in_flight;
                ++in_flight.rows;
                if (in_flight.rows > stageRowLimit(in_flight) || in_flight.rows > config.max_result_rows)
                {
                    failOperation(slot, DbFailureKind::ResultTooLarge, "row cap exceeded");
                    return false;
                }

                const unsigned long* lengths = ::mysql_fetch_lengths(slot.result);
                std::uint64_t row_bytes = 0;
                const unsigned int columns = ::mysql_num_fields(slot.result);
                for (unsigned int column = 0; column < columns; ++column)
                {
                    row_bytes += lengths == nullptr ? 0 : lengths[column];
                }
                bytes_used += row_bytes;
                in_flight.bytes += row_bytes;
                if (in_flight.bytes > config.max_result_bytes)
                {
                    failOperation(slot, DbFailureKind::ResultTooLarge, "byte cap exceeded");
                    return false;
                }

                if (!consumeRow(in_flight, slot.result, row, columns))
                {
                    failOperation(slot, DbFailureKind::QueryFailed, "row decode failed");
                    return false;
                }
                slot.runnable = true;
                return true;
            }

            case SlotState::FreeingResult:
            {
                const net_async_status status = ::mysql_free_result_nonblocking(slot.result);
                if (status == NET_ASYNC_NOT_READY)
                {
                    syncInterest(slot);
                    slot.runnable = false;
                    return false;
                }
                slot.result = nullptr;
                if (status == NET_ASYNC_ERROR)
                {
                    failOperation(slot, DbFailureKind::QueryFailed, "free result failed");
                    return false;
                }

                return advanceStage(slot);
            }
            }
            return false;
        }

        // Moves to the next statement of the operation, or finishes it.
        [[nodiscard]] bool advanceStage(Slot& slot)
        {
            InFlight& in_flight = *slot.in_flight;
            switch (in_flight.stage)
            {
            case Stage::PlayerRow:
                if (!in_flight.result.found)
                {
                    // No row means no player, and the skills query would be noise.
                    finish(slot, DbResult{std::move(in_flight.result)});
                    return true;
                }
                in_flight.stage = Stage::PlayerSkills;
                in_flight.rows = 0;
                beginStageQuery(slot);
                return true;

            case Stage::PlayerSkills:
                finish(slot, DbResult{std::move(in_flight.result)});
                return true;

            case Stage::SaveIsolation:
                in_flight.stage = Stage::SaveBegin;
                beginStageQuery(slot);
                return true;
            case Stage::SaveBegin:
                in_flight.stage = Stage::SaveUpsert;
                beginStageQuery(slot);
                return true;
            case Stage::SaveUpsert:
                in_flight.stage = Stage::SaveDeleteSkills;
                beginStageQuery(slot);
                return true;
            case Stage::SaveDeleteSkills:
                in_flight.stage = Stage::SaveInsertSkills;
                beginStageQuery(slot);
                return true;
            case Stage::SaveInsertSkills:
                in_flight.stage = Stage::SaveCommit;
                beginStageQuery(slot);
                return true;

            case Stage::SaveCommit:
                ++metrics.commits_acknowledged;
                finish(slot, DbResult{SavePlayerResult{.outcome = SaveOutcome::Committed}});
                return true;

            case Stage::SaveRollback:
                // The rollback was acknowledged, so the uncommitted work is gone and
                // the connection is still usable.
                ++metrics.rollbacks;
                finish(slot, DbResult{SavePlayerResult{.outcome = SaveOutcome::FailedBeforeCommit}});
                return true;
            }
            return false;
        }

        // A statement inside the transaction failed. A failed statement does not
        // roll the transaction back on its own, so an explicit ROLLBACK is sent.
        void onSaveStatementFailed(Slot& slot, std::string message)
        {
            InFlight& in_flight = *slot.in_flight;

            if (in_flight.commit_dispatched)
            {
                // COMMIT was already on its way. Whether it applied cannot be
                // established from here.
                ++metrics.commits_unknown;
                ++metrics.operations_failed;
                std::optional<InFlight> victim = std::move(slot.in_flight);
                slot.in_flight.reset();
                closeSlot(slot);
                slot.retry_at = Clock::now();
                sink.completeDb(victim->key, DbResult{SavePlayerResult{.outcome = SaveOutcome::CommitOutcomeUnknown}});
                return;
            }

            if (in_flight.stage == Stage::SaveRollback)
            {
                // The rollback itself could not be confirmed. Dropping the
                // connection discards the transaction, which reaches the same
                // meaning by a different route.
                ++metrics.operations_failed;
                std::optional<InFlight> victim = std::move(slot.in_flight);
                slot.in_flight.reset();
                closeSlot(slot);
                slot.retry_at = Clock::now();
                ++metrics.connections_poisoned;
                sink.completeDb(victim->key, DbResult{SavePlayerResult{.outcome = SaveOutcome::FailedBeforeCommit}});
                return;
            }

            static_cast<void>(message);
            in_flight.stage = Stage::SaveRollback;
            beginStageQuery(slot);
        }

        [[nodiscard]] static bool consumeRow(InFlight& in_flight, MYSQL_RES* result, MYSQL_ROW row, const unsigned int columns)
        {
            const unsigned long* lengths = ::mysql_fetch_lengths(result);
            if (lengths == nullptr)
            {
                return false;
            }

            if (in_flight.stage == Stage::PlayerSkills)
            {
                if (columns != 1)
                {
                    return false;
                }
                std::uint64_t skill_id = 0;
                if (!parseUnsigned(row[0], lengths[0], skill_id) || skill_id > std::numeric_limits<std::uint32_t>::max())
                {
                    return false;
                }
                in_flight.result.owned_skill_ids.push_back(static_cast<std::uint32_t>(skill_id));
                return true;
            }

            if (columns != PLAYER_ROW_COLUMNS || in_flight.result.found)
            {
                // A second row for a primary key means the schema is not what this
                // query shape assumes.
                return false;
            }

            // Columns 2 and 3 are signed INT; the rest are BIGINT UNSIGNED.
            constexpr unsigned int POSITION_X_COLUMN = 2;
            constexpr unsigned int POSITION_Y_COLUMN = 3;

            std::uint64_t values[PLAYER_ROW_COLUMNS] = {};
            std::int64_t positions[2] = {};
            bool present[PLAYER_ROW_COLUMNS] = {};
            for (unsigned int column = 0; column < columns; ++column)
            {
                present[column] = row[column] != nullptr;
                if (!present[column])
                {
                    continue;
                }
                if (column == POSITION_X_COLUMN || column == POSITION_Y_COLUMN)
                {
                    if (!parseSigned(row[column], lengths[column], positions[column - POSITION_X_COLUMN]))
                    {
                        return false;
                    }
                    continue;
                }
                if (!parseUnsigned(row[column], lengths[column], values[column]))
                {
                    return false;
                }
            }

            // zone_id, position_x and position_y are NULL together or not at all.
            const bool has_location = present[1] && present[2] && present[3];
            if ((present[1] || present[2] || present[3]) && !has_location)
            {
                return false;
            }
            if (!present[0] || !present[4] || !present[5] || !present[6] || !present[7])
            {
                return false;
            }
            if (positions[0] < std::numeric_limits<std::int32_t>::min() || positions[0] > std::numeric_limits<std::int32_t>::max() ||
                positions[1] < std::numeric_limits<std::int32_t>::min() || positions[1] > std::numeric_limits<std::int32_t>::max() ||
                values[7] > std::numeric_limits<std::uint32_t>::max())
            {
                return false;
            }

            in_flight.result.found = true;
            in_flight.result.row = LoadedPlayerRow{
                .player_id = std::get<LoadPlayerRequest>(in_flight.request).player_id,
                .handled_command_count = values[0],
                .has_location = has_location,
                .zone_id = has_location ? values[1] : 0,
                .position_x = has_location ? static_cast<std::int32_t>(positions[0]) : 0,
                .position_y = has_location ? static_cast<std::int32_t>(positions[1]) : 0,
                .currency_balance = values[4],
                .purchased_item_count = values[5],
                .street_experience = values[6],
                .equipped_skill_id = static_cast<std::uint32_t>(values[7]),
            };
            return true;
        }
    };

    DbClient::DbClient(DbClientConfig config, DbCompletionSink& sink)
        : _impl(std::make_unique<Impl>(std::move(config), sink))
    {
        if (!isValid(_impl->config))
        {
            throw std::invalid_argument{"Invalid DbClient configuration"};
        }
    }

    DbClient::~DbClient() = default;

    void DbClient::start(Poller& poller)
    {
        _impl->poller = &poller;
        for (Impl::Slot& slot : _impl->slots)
        {
            if (slot.state == Impl::SlotState::Closed)
            {
                _impl->openSlot(slot);
            }
        }
    }

    DbSubmitResult DbClient::tryStart(const AwaitKey key, DbRequest request, const DbTimePoint deadline)
    {
        if (_impl->shutting_down)
        {
            ++_impl->metrics.submit_rejections;
            return DbSubmitResult{.status = DbSubmitStatus::Rejected};
        }

        if (const auto* save = std::get_if<SavePlayerRequest>(&request))
        {
            // Same rejection the blocking repository makes, and the row cap the
            // shape promises.
            if (save->owned_skill_ids.empty() || save->owned_skill_ids.size() > MAX_PLAYER_SKILL_ROWS)
            {
                ++_impl->metrics.submit_rejections;
                return DbSubmitResult{.status = DbSubmitStatus::Rejected};
            }
        }

        for (Impl::Slot& slot : _impl->slots)
        {
            if (slot.state == Impl::SlotState::Idle && !slot.in_flight.has_value())
            {
                _impl->assign(
                    slot,
                    Impl::Queued{
                        .key = key,
                        .request = std::move(request),
                        .deadline = deadline,
                    }
                );
                return DbSubmitResult{.status = DbSubmitStatus::Pending};
            }
        }

        if (_impl->queue.size() >= _impl->config.max_queued_operations)
        {
            ++_impl->metrics.submit_rejections;
            return DbSubmitResult{.status = DbSubmitStatus::Rejected};
        }

        _impl->queue.push_back(Impl::Queued{
            .key = key,
            .request = std::move(request),
            .deadline = deadline,
        });
        return DbSubmitResult{.status = DbSubmitStatus::Pending};
    }

    void DbClient::onPollEvent(const PollToken token)
    {
        if (token.kind != PollTargetKind::DbConnection || token.index >= _impl->slots.size())
        {
            ++_impl->metrics.stale_poll_events;
            return;
        }

        Impl::Slot& slot = _impl->slots[token.index];
        // The token's 32 bits are a pre-filter; the slot's full 64-bit generation
        // decides. A reconnect can reuse the descriptor, so an event for a closed
        // incarnation must not progress the new one.
        if (static_cast<std::uint32_t>(slot.generation.value & 0xFFFFFFFFULL) != token.generation || slot.handle == nullptr)
        {
            ++_impl->metrics.stale_poll_events;
            return;
        }
        slot.runnable = true;
    }

    bool DbClient::advance(const DbProgressBudget& budget)
    {
        const auto started_at = Clock::now();
        std::size_t steps = 0;
        std::size_t rows = 0;
        std::uint64_t bytes = 0;

        bool progressed = true;
        while (progressed)
        {
            if (steps >= budget.max_steps || rows >= budget.max_rows || bytes >= budget.max_bytes || budgetExpired(started_at, budget.max_duration))
            {
                ++_impl->metrics.budget_yields;
                break;
            }

            progressed = false;
            for (Impl::Slot& slot : _impl->slots)
            {
                if (!slot.runnable)
                {
                    continue;
                }
                ++steps;
                if (_impl->step(slot, rows, bytes))
                {
                    progressed = true;
                }
                _impl->pumpQueue();
            }
        }

        return hasLocalWork();
    }

    bool DbClient::hasLocalWork() const noexcept
    {
        return std::any_of(
            _impl->slots.begin(),
            _impl->slots.end(),
            [](const Impl::Slot& slot)
            {
                return slot.runnable;
            }
        );
    }

    std::size_t DbClient::inFlightCount() const noexcept
    {
        return static_cast<std::size_t>(std::count_if(
            _impl->slots.begin(),
            _impl->slots.end(),
            [](const Impl::Slot& slot)
            {
                return slot.in_flight.has_value();
            }
        ));
    }

    std::size_t DbClient::queuedCount() const noexcept
    {
        return _impl->queue.size();
    }

    void DbClient::expireDeadlines(const DbTimePoint now)
    {
        // A queued request never reached the server, so its connection is fine and
        // must not be torn down. Killing connections on queue pressure is how an
        // overloaded worker would take itself apart.
        for (auto it = _impl->queue.begin(); it != _impl->queue.end();)
        {
            if (it->deadline > now)
            {
                ++it;
                continue;
            }
            const AwaitKey key = it->key;
            it = _impl->queue.erase(it);
            ++_impl->metrics.queued_timeouts;
            ++_impl->metrics.operations_failed;
            _impl->sink.completeDb(
                key,
                DbResult{DbFailure{
                    .kind = DbFailureKind::TimedOut,
                    .reached_server = false,
                    .message = "timed out while queued",
                }}
            );
        }

        for (Impl::Slot& slot : _impl->slots)
        {
            if (!slot.in_flight.has_value() || slot.in_flight->deadline > now)
            {
                continue;
            }
            ++_impl->metrics.in_flight_timeouts;
            _impl->poisonSlot(
                slot,
                DbFailure{
                    .kind = DbFailureKind::TimedOut,
                    .reached_server = true,
                    .message = "timed out in flight",
                }
            );
        }
    }

    void DbClient::maintainConnections(const DbTimePoint now)
    {
        if (_impl->shutting_down)
        {
            return;
        }
        for (Impl::Slot& slot : _impl->slots)
        {
            if (slot.state != Impl::SlotState::Closed || slot.retry_at > now)
            {
                continue;
            }
            _impl->openSlot(slot);
        }
    }

    void DbClient::beginShutdown() noexcept
    {
        _impl->shutting_down = true;

        while (!_impl->queue.empty())
        {
            const AwaitKey key = _impl->queue.front().key;
            _impl->queue.pop_front();
            ++_impl->metrics.operations_failed;
            _impl->sink.completeDb(
                key,
                DbResult{DbFailure{
                    .kind = DbFailureKind::Overloaded,
                    .reached_server = false,
                    .message = "worker is shutting down",
                }}
            );
        }
    }

    bool DbClient::shuttingDown() const noexcept
    {
        return _impl->shutting_down;
    }

    void DbClient::shutdown(Poller& poller)
    {
        _impl->shutting_down = true;
        _impl->poller = &poller;
        for (Impl::Slot& slot : _impl->slots)
        {
            if (slot.in_flight.has_value())
            {
                const Impl::InFlight victim = std::move(*slot.in_flight);
                slot.in_flight.reset();
                ++_impl->metrics.operations_failed;
                if (std::holds_alternative<SavePlayerRequest>(victim.request) && victim.commit_dispatched)
                {
                    ++_impl->metrics.commits_unknown;
                }
                _impl->sink.completeDb(
                    victim.key,
                    Impl::failureResultFor(
                        victim,
                        DbFailure{
                            .kind = DbFailureKind::ConnectionLost,
                            .reached_server = true,
                            .message = "worker is shutting down",
                        }
                    )
                );
            }
            _impl->closeSlot(slot);
        }
        _impl->poller = nullptr;
    }

    const DbClientMetrics& DbClient::metrics() const noexcept
    {
        return _impl->metrics;
    }
}
