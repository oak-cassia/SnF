#pragma once

#include "snf/game/player_command.hpp"
#include "snf/game/player_record.hpp"
#include "snf/game/player_result.hpp"

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
        Skills = 1U << 3U,
    };

    using PlayerStateComponentMask = std::uint8_t;

    [[nodiscard]] constexpr PlayerStateComponentMask componentMask(const PlayerStateComponent component) noexcept
    {
        return static_cast<PlayerStateComponentMask>(component);
    }

    inline constexpr std::size_t DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY = 1024;

    class PlayerState
    {
    public:
        [[nodiscard]] std::optional<PlayerId> identity() const noexcept;
        [[nodiscard]] std::uint64_t handledCommandCount() const noexcept;
        [[nodiscard]] std::optional<PlayerLocation> lastLocation() const noexcept;
        [[nodiscard]] std::uint64_t currencyBalance() const noexcept;
        [[nodiscard]] std::uint64_t purchasedItemCount() const noexcept;
        [[nodiscard]] std::uint64_t streetExperience() const noexcept;
        [[nodiscard]] const SkillLoadout& getSkillLoadout() const noexcept;
        [[nodiscard]] PlayerStateComponentMask dirtyComponents() const noexcept;

    private:
        friend class Player;

        struct Session
        {
            std::optional<PlayerId> identity;
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

        struct Skills
        {
            SkillLoadout skill_loadout{};
        } _skills;

        PlayerStateComponentMask _dirty_components{0};
    };

    class Player
    {
    public:
        Player() = default;
        explicit Player(std::optional<PlayerId> identity) noexcept;
        Player(std::optional<PlayerId> identity, std::size_t max_purchase_idempotency_records);

        Player(const Player&) = delete;
        Player& operator=(const Player&) = delete;
        Player(Player&&) noexcept = default;
        Player& operator=(Player&&) noexcept = default;

        [[nodiscard]] const PlayerState& state() const noexcept;
        void restore(const PlayerRecord& record);
        void setLastLocation(std::optional<PlayerLocation> location) noexcept;
        void grantStreetExperience(std::uint64_t experience) noexcept;
        [[nodiscard]] bool hasFlushableDirtyState() const noexcept;
        [[nodiscard]] PlayerStateComponentMask dirtyComponents() const noexcept;
        [[nodiscard]] std::optional<PlayerRecord> takeDirtySnapshot(PlayerStateComponentMask* cleared_components = nullptr);
        void restoreDirtyComponents(PlayerStateComponentMask components) noexcept;
        [[nodiscard]] PlayerRecord snapshot() const;

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
        [[nodiscard]] PlayerResult handleCommand(const JoinRoomRequest& command);

        PlayerState _state;
        std::size_t _max_purchase_idempotency_records{DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY};
        std::unordered_map<std::uint64_t, PurchaseEvidence> _purchase_evidence;
    };
}
