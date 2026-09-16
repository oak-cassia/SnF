#pragma once

#include "snf/worker/connection.hpp"
#include "snf/worker/poll_token.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace snf::worker
{
    struct PollRegistrationGeneration
    {
        std::uint64_t value{0};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const PollRegistrationGeneration&) const noexcept = default;
    };

    struct PollRegistrationHandle
    {
        std::uint32_t index{0};
        PollRegistrationGeneration generation{};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return generation.isValid();
        }

        [[nodiscard]] bool operator==(const PollRegistrationHandle&) const noexcept = default;
    };

    struct PollRegistrationView
    {
        PollRegistrationHandle handle;
        int descriptor{-1};
        PollToken token;
        std::optional<ConnectionHandle> connection;
    };

    // The table owns only the small registration records. Kernel epoll state
    // is still owned by Poller. This separation makes admission transactional
    // and lets stale tokens be rejected before a socket is touched.
    class PollRegistrationTable final
    {
    public:
        class Reservation final
        {
        public:
            Reservation() = delete;
            ~Reservation();

            Reservation(const Reservation&) = delete;
            Reservation& operator=(const Reservation&) = delete;

            Reservation(Reservation&& other) noexcept;
            Reservation& operator=(Reservation&& other) noexcept;

            [[nodiscard]] PollRegistrationHandle handle() const noexcept;
            [[nodiscard]] PollRegistrationView view() const;
            [[nodiscard]] PollToken token() const noexcept;

            // Commit publishes the record to lookup(). Destruction without a
            // commit rolls back the reservation and returns its slot.
            void commit() noexcept;
            void rollback() noexcept;

            // Only PollRegistrationTable creates meaningful reservations;
            // the public constructor exists so std::optional can in-place
            // construct the move-only value.
            Reservation(PollRegistrationTable& table, std::size_t index, PollRegistrationHandle handle) noexcept;

        private:
            friend class PollRegistrationTable;

            PollRegistrationTable* _table;
            std::size_t _index;
            PollRegistrationHandle _handle;
            bool _committed{false};
        };

        explicit PollRegistrationTable(std::size_t capacity);

        PollRegistrationTable(const PollRegistrationTable&) = delete;
        PollRegistrationTable& operator=(const PollRegistrationTable&) = delete;

        [[nodiscard]] std::optional<Reservation> tryReserve(
            int descriptor,
            PollTargetKind kind,
            std::optional<ConnectionHandle> connection = std::nullopt
        );

        [[nodiscard]] std::optional<PollRegistrationView> lookup(PollToken token) const noexcept;
        [[nodiscard]] std::optional<PollRegistrationView> lookup(PollRegistrationHandle handle) const noexcept;

        [[nodiscard]] bool release(PollRegistrationHandle handle) noexcept;
        [[nodiscard]] bool hasCapacity() const noexcept;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] std::size_t reservedCount() const noexcept;
        [[nodiscard]] std::size_t activeCount() const noexcept;

    private:
        struct Slot
        {
            std::uint64_t generation{0};
            int descriptor{-1};
            PollToken token{};
            std::optional<ConnectionHandle> connection;
            bool reserved{false};
            bool committed{false};
        };

        [[nodiscard]] PollRegistrationView viewFor(std::size_t index) const noexcept;
        void rollback(std::size_t index) noexcept;

        std::vector<Slot> _slots;
        std::size_t _reserved_count{0};
        std::size_t _active_count{0};
        std::uint64_t _next_generation{0};
    };
}
