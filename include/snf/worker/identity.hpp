#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace snf::worker
{
    // ActorKind enum의 숫자는 stableActorHash()와 ownerOf() 결과에 직접 들어가므로,
    // 앞으로 ActorKind를 추가할 때 기존 번호는 절대 바꾸지 않는다.
    enum class ActorKind : std::uint8_t
    {
        ProvisionalPlayer = 0,
        Player = 1,
        Zone = 2,
        Room = 3,
    };

    using EntityId = std::uint64_t;

    struct WorkerId
    {
        std::uint16_t value{0};

        [[nodiscard]] bool operator==(const WorkerId&) const noexcept = default;
    };

    struct ActorIncarnation
    {
        std::uint64_t value{0};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const ActorIncarnation&) const noexcept = default;
    };

    struct OperationId
    {
        std::uint64_t value{0};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const OperationId&) const noexcept = default;
    };

    struct ConnectionId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const ConnectionId&) const noexcept = default;
    };

    struct ConnectionGeneration
    {
        std::uint64_t value{0};

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const ConnectionGeneration&) const noexcept = default;
    };

    struct ActorKey
    {
        ActorKind kind;
        EntityId entity;

        [[nodiscard]] bool operator==(const ActorKey&) const noexcept = default;
    };

    struct ActivationRef
    {
        ActorKey actor;
        ActorIncarnation incarnation;

        [[nodiscard]] bool operator==(const ActivationRef&) const noexcept = default;
    };

    struct ConnectionRef
    {
        ConnectionId id;
        ConnectionGeneration generation;
        WorkerId owner;

        [[nodiscard]] bool operator==(const ConnectionRef&) const noexcept = default;
    };

    // 포함: ActorKey, incarnation, operation id
    // 제외: WorkerId, deadline, retry policy, result payload
    // completion route는 호출자의 Worker context가 정하고, deadline은 ActorSlot.blocked가 소유한다.
    struct AwaitKey
    {
        ActorKey actor;
        ActorIncarnation incarnation;
        OperationId operation;

        [[nodiscard]] bool operator==(const AwaitKey&) const noexcept = default;
    };

    static_assert(std::is_trivially_copyable_v<ActorKey>);
    static_assert(std::is_trivially_copyable_v<ActivationRef>);
    static_assert(std::is_trivially_copyable_v<ConnectionRef>);
    static_assert(std::is_trivially_copyable_v<AwaitKey>);

    // Worker placement의 기준 함수다. std::size_t나 std::hash를 거치지 않는다.
    // 값이 바뀌면 모든 Actor의 Worker 배치가 바뀌므로 golden vector 테스트로 고정한다.
    [[nodiscard]] constexpr std::uint64_t stableActorHash(const ActorKey key, const std::uint64_t placement_seed) noexcept
    {
        std::uint64_t value = key.entity;
        value ^= static_cast<std::uint64_t>(key.kind) * 0x9e3779b97f4a7c15ULL;
        value ^= placement_seed;
        value ^= value >> 30U;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27U;
        value *= 0x94d049bb133111ebULL;
        value ^= value >> 31U;
        return value;
    }

    struct ActorKeyHash
    {
        [[nodiscard]] std::size_t operator()(const ActorKey key) const noexcept
        {
            return static_cast<std::size_t>(stableActorHash(key, 0));
        }
    };

    // AwaitKey용 기본 hash는 제공하지 않는다.
    // Worker-level PendingOperationTable을 우발적으로 만들지 않게 하고,
    // completion identity는 ActorSlot.blocked에서 직접 검증한다.

    // Worker-local counter다. owner Worker만 접근하므로 std::atomic을 쓰지 않는다(INV-01).
    template <class Id> class SequenceSource
    {
    public:
        [[nodiscard]] Id next() noexcept
        {
            ++_last;
            return Id{_last};
        }

        [[nodiscard]] Id last() const noexcept
        {
            return Id{_last};
        }

    private:
        std::uint64_t _last{0};
    };

    using IncarnationSource = SequenceSource<ActorIncarnation>;
    using OperationIdSource = SequenceSource<OperationId>;
    using ConnectionGenerationSource = SequenceSource<ConnectionGeneration>;

    // Actor reference에 owner를 저장하지 않고 매번 계산한다(§2.2).
    // precondition: worker_count > 0. startup configuration에서 검증한다(§10.1).
    // NDEBUG 빌드에서는 assert가 사라지고 % 0이 UB가 되므로,
    // 2단계 Worker skeleton의 startup 설정 검증에서 worker_count == 0을 반드시 거부해야 한다.
    [[nodiscard]] constexpr WorkerId ownerOf(const ActorKey key, const std::uint16_t worker_count, const std::uint64_t placement_seed) noexcept
    {
        assert(worker_count > 0);

        const std::uint64_t hash = stableActorHash(key, placement_seed);
        return WorkerId{static_cast<std::uint16_t>(hash % worker_count)};
    }
}
