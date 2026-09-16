#include "snf/game/skill_loadout.hpp"

import snf.game.skill_catalog;

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace
{
    constexpr auto BY_SKILL_ID = [](const snf::server::SkillId left, const snf::server::SkillId right)
    {
        return left.value < right.value;
    };
}

namespace snf::server
{
    SkillLoadout::SkillLoadout()
        : _owned_skill_ids{SLASH}
        , _equipped_skill_id(SLASH)
    {
    }

    SkillLoadout::SkillLoadout(std::vector<SkillId> owned_skill_ids, const SkillId equipped_skill_id)
        : _owned_skill_ids(std::move(owned_skill_ids))
        , _equipped_skill_id(equipped_skill_id)
    {
        std::ranges::sort(_owned_skill_ids, BY_SKILL_ID);
        if (_owned_skill_ids.empty())
        {
            throw std::invalid_argument{"A SkillLoadout must own at least one skill ID"};
        }
        for (std::size_t index = 0; index < _owned_skill_ids.size(); ++index)
        {
            const SkillId skill_id = _owned_skill_ids[index];
            if (!findSkill(skill_id))
            {
                throw std::invalid_argument{"A SkillLoadout contains an unknown skill ID"};
            }
            if (index != 0 && _owned_skill_ids[index - 1] == skill_id)
            {
                throw std::invalid_argument{"A SkillLoadout contains a duplicate skill ID"};
            }
        }
        if (!hasOwnedSkillId(_equipped_skill_id))
        {
            throw std::invalid_argument{"The equipped skill ID must be owned"};
        }
    }

    std::span<const SkillId> SkillLoadout::getOwnedSkillIds() const noexcept
    {
        return _owned_skill_ids;
    }

    SkillId SkillLoadout::getEquippedSkillId() const noexcept
    {
        return _equipped_skill_id;
    }

    bool SkillLoadout::hasOwnedSkillId(const SkillId skill_id) const noexcept
    {
        const auto position = std::ranges::lower_bound(_owned_skill_ids, skill_id, BY_SKILL_ID);
        return position != _owned_skill_ids.end() && *position == skill_id;
    }

    AddOwnedSkillResult SkillLoadout::addOwnedSkillId(const SkillId skill_id)
    {
        if (!findSkill(skill_id))
        {
            return AddOwnedSkillResult::UnknownSkill;
        }
        const auto position = std::ranges::lower_bound(_owned_skill_ids, skill_id, BY_SKILL_ID);
        if (position != _owned_skill_ids.end() && *position == skill_id)
        {
            return AddOwnedSkillResult::AlreadyOwned;
        }
        _owned_skill_ids.insert(position, skill_id);
        return AddOwnedSkillResult::Added;
    }

    EquipSkillResult SkillLoadout::equipSkillId(const SkillId skill_id) noexcept
    {
        if (!findSkill(skill_id))
        {
            return EquipSkillResult::UnknownSkill;
        }
        if (!hasOwnedSkillId(skill_id))
        {
            return EquipSkillResult::SkillNotOwned;
        }
        if (_equipped_skill_id == skill_id)
        {
            return EquipSkillResult::AlreadyEquipped;
        }
        _equipped_skill_id = skill_id;
        return EquipSkillResult::Equipped;
    }
}
