#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct PartyId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const PartyId&) const noexcept = default;
    };

    struct PartyIdHash
    {
        [[nodiscard]] std::size_t operator()(const PartyId party) const noexcept
        {
            return std::hash<std::uint64_t>{}(party.value);
        }
    };
}
