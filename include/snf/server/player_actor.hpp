#pragma once

#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_command.hpp"
#include "snf/server/player_record.hpp"
#include "snf/server/player_result.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>

namespace snf::server
{
    enum class PlayerStateComponent : std::uint8_t
    {
        Session = 1U << 0U,
        Economy = 1U << 1U,
        Progression = 1U << 2U,
    };

    using PlayerStateComponentMask = std::uint8_t;

    [[nodiscard]] constexpr PlayerStateComponentMask componentMask(const PlayerStateComponent component) noexcept
    {
        return static_cast<PlayerStateComponentMask>(component);
    }

    inline constexpr std::size_t DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY = 1024;

    // The state is intentionally only mutable by PlayerActor. These are ownership
    // boundaries inside one Actor, not additional Actors.
    class PlayerState
    {
    public:
        [[nodiscard]] PlayerActorId identity() const noexcept;
        [[nodiscard]] std::uint64_t handledCommandCount() const noexcept;
        [[nodiscard]] std::optional<PlayerLocation> lastLocation() const noexcept;
        [[nodiscard]] std::uint64_t currencyBalance() const noexcept;
        [[nodiscard]] std::uint64_t purchasedItemCount() const noexcept;
        [[nodiscard]] std::uint64_t streetExperience() const noexcept;
        [[nodiscard]] PlayerStateComponentMask dirtyComponents() const noexcept;

    private:
        friend class PlayerActor;

        struct Session
        {
            PlayerActorId identity;
            std::uint64_t handled_command_count{0};
            std::optional<PlayerLocation> last_location;
        } _session;

        struct Economy
        {
            std::uint64_t currency_balance{INITIAL_CURRENCY_BALANCE};
            std::uint64_t purchased_item_count{0};
        } _economy;

        struct Progression
        {
            std::uint64_t street_experience{0};
        } _progression;

        PlayerStateComponentMask _dirty_components{0};
    };

    class PlayerActor
    {
    public:
        PlayerActor() = default;
        explicit PlayerActor(PlayerActorId identity) noexcept;
        PlayerActor(PlayerActorId identity, std::size_t max_purchase_idempotency_records);

        PlayerActor(const PlayerActor&) = delete;
        PlayerActor& operator=(const PlayerActor&) = delete;
        PlayerActor(PlayerActor&&) noexcept = default;
        PlayerActor& operator=(PlayerActor&&) noexcept = default;

        // Only a const view escapes the actor, but it is safe to read only on the
        // owning Worker. Cross-thread queries must use an immutable snapshot or a
        // command; const does not provide synchronization.
        [[nodiscard]] const PlayerState& state() const noexcept;
        void restore(const PlayerRecord& record);
        void setLastLocation(std::optional<PlayerLocation> location) noexcept;
        void grantStreetExperience(std::uint64_t experience) noexcept;
        [[nodiscard]] bool hasFlushableDirtyState() const noexcept;
        [[nodiscard]] PlayerStateComponentMask dirtyComponents() const noexcept;
        // Must be called on the owning Worker. It clears all currently dirty
        // components and returns a flat persistence record for the service queue.
        [[nodiscard]] std::optional<PlayerRecord> takeDirtySnapshot(PlayerStateComponentMask* cleared_components = nullptr);
        void restoreDirtyComponents(PlayerStateComponentMask components) noexcept;
        [[nodiscard]] PlayerRecord snapshot() const;

        // Synchronous, and returns only decisions. Everything that has to wait --
        // loading a record, saving one, acquiring outbound capacity -- is awaited by
        // PlayerActorBinding around this call, which is what lets the handler stay a
        // plain function of its command and the state it owns.
        [[nodiscard]] PlayerResult handle(const PlayerCommand& command);

    private:
        struct PurchaseEvidence
        {
            ProductId product;
            PurchaseTransactionResult result;
        };

        [[nodiscard]] PlayerResult handleCommand(const PingCommand& command);
        [[nodiscard]] PlayerResult handleCommand(const AuthenticateCommand& command);
        [[nodiscard]] PlayerResult handleCommand(const PurchaseCommand& command);

        PlayerState _state;
        std::size_t _max_purchase_idempotency_records{DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY};
        std::unordered_map<std::uint64_t, PurchaseEvidence> _purchase_evidence;
    };
}
