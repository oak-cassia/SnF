#pragma once

#include "snf/game/skill_id.hpp"

#include <span>
#include <vector>

namespace snf::server
{
    enum class AddOwnedSkillResult
    {
        Added,
        AlreadyOwned,
        UnknownSkill,
    };

    enum class EquipSkillResult
    {
        Equipped,
        AlreadyEquipped,
        SkillNotOwned,
        UnknownSkill,
    };

    class SkillLoadout final
    {
    public:
        SkillLoadout();
        SkillLoadout(std::vector<SkillId> owned_skill_ids, SkillId equipped_skill_id);

        [[nodiscard]] std::span<const SkillId> getOwnedSkillIds() const noexcept;
        [[nodiscard]] SkillId getEquippedSkillId() const noexcept;
        [[nodiscard]] bool hasOwnedSkillId(SkillId skill_id) const noexcept;

        [[nodiscard]] AddOwnedSkillResult addOwnedSkillId(SkillId skill_id);
        [[nodiscard]] EquipSkillResult equipSkillId(SkillId skill_id) noexcept;

        [[nodiscard]] bool operator==(const SkillLoadout&) const noexcept = default;

    private:
        std::vector<SkillId> _owned_skill_ids;
        SkillId _equipped_skill_id{};
    };
}
