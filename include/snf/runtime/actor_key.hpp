#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::runtime
{
    // An actor's routing identity is deliberately independent from its domain
    // model. A numeric id can therefore be reused by different actor kinds
    // without sharing a mailbox or an affinity shard.
    enum class ActorKind : std::uint8_t
    {
        ProvisionalPlayer,
        Player,
        Zone,
    };

    using EntityId = std::uint64_t;

    struct ActorKey
    {
        ActorKind kind;
        EntityId entity;

        [[nodiscard]] bool operator==(const ActorKey&) const noexcept = default;
    };

    struct ActorKeyHash
    {
        [[nodiscard]] std::size_t operator()(const ActorKey& key) const noexcept
        {
            // The mix is stable for a process and includes the actor kind before
            // sharding. The scheduler always applies this exact hash modulo its
            // worker count; it never shards by an entity id alone.
            const std::uint64_t kind = static_cast<std::uint64_t>(key.kind);
            std::uint64_t value = key.entity + 0x9e3779b97f4a7c15ULL + (kind << 6U) + (kind >> 2U);
            value ^= value >> 30U;
            value *= 0xbf58476d1ce4e5b9ULL;
            value ^= value >> 27U;
            value *= 0x94d049bb133111ebULL;
            value ^= value >> 31U;
            return std::hash<std::uint64_t>{}(value);
        }
    };
}
