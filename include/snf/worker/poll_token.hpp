#pragma once

#include <cassert>
#include <cstdint>

namespace snf::worker
{
    enum class PollTargetKind : std::uint8_t
    {
        Invalid = 0, // zero-initialized 토큰을 거부하기 위한 sentinel
        Wakeup = 1,
        ClientConnection = 2,
        DbConnection = 3,
    };

    inline constexpr std::uint32_t MAX_POLL_INDEX = 0xFFFFFFu;        // 16,777,215 (24 bits)
    inline constexpr std::uint32_t MAX_POLL_GENERATION = 0xFFFFFFFFu; // 32 bits

    struct PollToken
    {
        PollTargetKind kind{PollTargetKind::Invalid};
        std::uint32_t index{0};
        std::uint32_t generation{0};

        [[nodiscard]] bool operator==(const PollToken&) const noexcept = default;
    };

    // epoll_event.data.u64 비트 배치:
    // bits 63..56 : kind        (8 bits)
    // bits 55..32 : index       (24 bits, 최대 16,777,215)
    // bits 31..0  : generation  (32 bits)
    //
    // ConnectionGeneration은 64-bit인데 토큰에는 하위 32비트만 싣는다.
    // 토큰의 generation은 "이 slot의 이전 incarnation에 대한 event인가"를 거르는 빠른 pre-filter이고,
    // 최종 판정은 slot을 조회한 뒤 전체 64-bit 값으로 한다.
    // 2^32회 재사용 안에서만 유효하며, 이는 OperationId wraparound와 같은 근거로 운영상 불가능한 범위다.
    // precondition: index <= MAX_POLL_INDEX (설정 invariant)
    [[nodiscard]] constexpr std::uint64_t encodePollToken(
        const PollTargetKind kind,
        const std::uint32_t index,
        const std::uint32_t generation
    ) noexcept
    {
        assert(index <= MAX_POLL_INDEX);
        return (static_cast<std::uint64_t>(kind) << 56U) | (static_cast<std::uint64_t>(index) << 32U) | static_cast<std::uint64_t>(generation);
    }

    [[nodiscard]] constexpr PollToken decodePollToken(const std::uint64_t raw) noexcept
    {
        return PollToken{
            .kind = static_cast<PollTargetKind>((raw >> 56U) & 0xFFULL),
            .index = static_cast<std::uint32_t>((raw >> 32U) & 0xFFFFFFULL),
            .generation = static_cast<std::uint32_t>(raw & 0xFFFFFFFFULL),
        };
    }
}
