#pragma once

#include "snf/worker/actor.hpp"
#include "snf/worker/identity.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace snf::worker
{
    struct MailboxUsage
    {
        std::size_t message_count{0};
        std::uint64_t charged_bytes{0};

        [[nodiscard]] bool operator==(const MailboxUsage&) const noexcept = default;
    };

    class ActorSlot final
    {
    public:
        ActorSlot(ActorKey key, ActorIncarnation incarnation, std::size_t slot_index) noexcept;

        ActorSlot(const ActorSlot&) = delete;
        ActorSlot& operator=(const ActorSlot&) = delete;
        ActorSlot(ActorSlot&&) noexcept = default;
        ActorSlot& operator=(ActorSlot&&) noexcept = default;

        [[nodiscard]] ActorKey key() const noexcept
        {
            return _key;
        }

        [[nodiscard]] ActorIncarnation incarnation() const noexcept
        {
            return _incarnation;
        }

        [[nodiscard]] std::size_t slotIndex() const noexcept
        {
            return _slot_index;
        }

        [[nodiscard]] ActorHandle handle() const noexcept
        {
            return ActorHandle{
                .slot_index = static_cast<std::uint32_t>(_slot_index),
                .incarnation = _incarnation,
            };
        }

        [[nodiscard]] ActivationRef activationRef() const noexcept
        {
            return ActivationRef{
                .actor = _key,
                .incarnation = _incarnation,
            };
        }

        [[nodiscard]] ActorState state() const noexcept
        {
            return _state;
        }

        void setState(const ActorState state) noexcept
        {
            _state = state;
        }

        [[nodiscard]] Mailbox& mailbox() noexcept
        {
            return _mailbox;
        }

        [[nodiscard]] const Mailbox& mailbox() const noexcept
        {
            return _mailbox;
        }

        [[nodiscard]] MailboxUsage mailboxUsage() const noexcept
        {
            return MailboxUsage{
                .message_count = _mailbox.size(),
                .charged_bytes = _mailbox.chargedBytes(),
            };
        }

        void clearMailbox() noexcept
        {
            _mailbox.clear();
        }

        [[nodiscard]] ActorInstance* instance() noexcept
        {
            return _instance.get();
        }

        [[nodiscard]] const ActorInstance* instance() const noexcept
        {
            return _instance.get();
        }

        void setInstance(std::unique_ptr<ActorInstance> instance) noexcept
        {
            _instance = std::move(instance);
        }

        [[nodiscard]] bool hasInstance() const noexcept
        {
            return _instance != nullptr;
        }

        [[nodiscard]] bool hasBlocked() const noexcept
        {
            return _blocked.has_value();
        }

        [[nodiscard]] const std::optional<BlockedTask>& blocked() const noexcept
        {
            return _blocked;
        }

        [[nodiscard]] std::optional<BlockedTask>& blocked() noexcept
        {
            return _blocked;
        }

        void setBlocked(BlockedTask blocked)
        {
            _blocked = std::move(blocked);
        }

        void clearBlocked() noexcept
        {
            _blocked.reset();
        }

    private:
        ActorKey _key;
        ActorIncarnation _incarnation;
        std::size_t _slot_index;
        ActorState _state{ActorState::Idle};
        std::unique_ptr<ActorInstance> _instance{nullptr};
        std::optional<BlockedTask> _blocked{std::nullopt};
        Mailbox _mailbox{};
    };

    class ReadyActorQueue final
    {
    public:
        explicit ReadyActorQueue(std::size_t capacity);

        [[nodiscard]] bool empty() const noexcept
        {
            return _size == 0;
        }

        [[nodiscard]] bool full() const noexcept
        {
            return _size == _capacity;
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return _size;
        }

        [[nodiscard]] std::size_t capacity() const noexcept
        {
            return _capacity;
        }

        void push(ActorHandle handle);
        [[nodiscard]] ActorHandle pop();
        void clear() noexcept;

    private:
        std::size_t _capacity;
        std::vector<ActorHandle> _buffer;
        std::size_t _head{0};
        std::size_t _tail{0};
        std::size_t _size{0};
    };

    class ActorTable final
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

            [[nodiscard]] ActorHandle handle() const noexcept;
            [[nodiscard]] ActorKey key() const noexcept;
            [[nodiscard]] ActorSlot& slot() noexcept;
            [[nodiscard]] const ActorSlot& slot() const noexcept;

            void commit() noexcept;
            void rollback() noexcept;

            Reservation(ActorTable& table, std::size_t index, ActorHandle handle) noexcept;

        private:
            friend class ActorTable;

            ActorTable* _table{nullptr};
            std::size_t _index{0};
            ActorHandle _handle{};
            bool _committed{false};
        };

        explicit ActorTable(std::size_t capacity);

        ActorTable(const ActorTable&) = delete;
        ActorTable& operator=(const ActorTable&) = delete;

        [[nodiscard]] std::optional<Reservation> tryReserve(ActorKey key);
        [[nodiscard]] ActorSlot* find(ActorKey key) noexcept;
        [[nodiscard]] const ActorSlot* find(ActorKey key) const noexcept;
        [[nodiscard]] ActorSlot* find(ActorHandle handle) noexcept;
        [[nodiscard]] const ActorSlot* find(ActorHandle handle) const noexcept;

        [[nodiscard]] bool release(ActorHandle handle) noexcept;

        [[nodiscard]] bool hasCapacity() const noexcept;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] std::size_t reservedCount() const noexcept;
        [[nodiscard]] std::size_t activeCount() const noexcept;
        [[nodiscard]] std::size_t availableCount() const noexcept;
        [[nodiscard]] std::vector<ActorHandle> activeHandles() const;

    private:
        struct Slot
        {
            std::optional<ActorSlot> actor;
            bool reserved{false};
            bool committed{false};
        };

        [[nodiscard]] ActorIncarnation nextIncarnation();
        void rollback(std::size_t index) noexcept;

        std::vector<Slot> _slots;
        std::vector<std::size_t> _free_indices;
        std::unordered_map<ActorKey, std::size_t, ActorKeyHash> _key_to_slot;
        IncarnationSource _incarnation_source;
        std::size_t _reserved_count{0};
        std::size_t _active_count{0};
    };
}
