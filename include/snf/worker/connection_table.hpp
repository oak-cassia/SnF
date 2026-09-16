#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/poll_token.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace snf::worker
{
    struct ConnectionTableConfig
    {
        std::size_t capacity{1024};
        ConnectionLimits limits{};
    };

    class ConnectionTable final
    {
    public:
        class Reservation final
        {
        public:
            ~Reservation();

            Reservation(const Reservation&) = delete;
            Reservation& operator=(const Reservation&) = delete;

            Reservation(Reservation&& other) noexcept;
            Reservation& operator=(Reservation&& other) noexcept;

            [[nodiscard]] ConnectionHandle handle() const noexcept;
            [[nodiscard]] ConnectionRef reference() const noexcept;
            [[nodiscard]] ConnectionSlot& slot() noexcept;
            [[nodiscard]] const ConnectionSlot& slot() const noexcept;

            void commit() noexcept;
            void rollback() noexcept;

            // Only ConnectionTable creates meaningful reservations;
            // the public constructor exists so std::optional can in-place
            // construct the move-only value.
            Reservation(ConnectionTable& table, std::size_t index, ConnectionHandle handle) noexcept;

        private:
            friend class ConnectionTable;

            ConnectionTable* _table;
            std::size_t _index;
            ConnectionHandle _handle;
            bool _committed{false};
        };

        explicit ConnectionTable(const ConnectionTableConfig& config);

        ConnectionTable(const ConnectionTable&) = delete;
        ConnectionTable& operator=(const ConnectionTable&) = delete;

        [[nodiscard]] std::optional<Reservation> tryReserve(snf::net::UniqueFileDescriptor socket, WorkerId owner);

        [[nodiscard]] ConnectionSlot* find(ConnectionHandle handle) noexcept;
        [[nodiscard]] const ConnectionSlot* find(ConnectionHandle handle) const noexcept;
        [[nodiscard]] bool release(ConnectionHandle handle) noexcept;

        [[nodiscard]] bool hasCapacity() const noexcept;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] std::size_t reservedCount() const noexcept;
        [[nodiscard]] std::size_t activeCount() const noexcept;
        [[nodiscard]] std::size_t availableCount() const noexcept;
        [[nodiscard]] std::vector<ConnectionHandle> activeHandles() const;

    private:
        struct Slot
        {
            std::optional<ConnectionSlot> connection;
            bool reserved{false};
            bool committed{false};
        };

        [[nodiscard]] ConnectionGeneration nextGeneration();
        void rollback(std::size_t index) noexcept;

        std::vector<Slot> _slots;
        ConnectionLimits _limits;
        ConnectionGenerationSource _generation_source;
        std::size_t _reserved_count{0};
        std::size_t _active_count{0};
    };
}
