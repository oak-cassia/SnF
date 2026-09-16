#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

namespace snf::protocol
{
    // 와이어 상의 모든 정수는 big-endian이다. 폭은 Integer의 크기가 결정한다.
    template <typename Integer> void append_big_endian(std::vector<std::byte>& bytes, const Integer value)
    {
        constexpr std::uint64_t BYTE_MASK = 0xFFU;
        for (std::size_t remaining = sizeof(Integer); remaining > 0; --remaining)
        {
            const std::size_t shift = (remaining - 1) * 8;
            bytes.push_back(static_cast<std::byte>((static_cast<std::make_unsigned_t<Integer>>(value) >> shift) & BYTE_MASK));
        }
    }

    inline void append_u16(std::vector<std::byte>& bytes, const std::uint16_t value)
    {
        append_big_endian(bytes, value);
    }

    inline void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value)
    {
        append_big_endian(bytes, value);
    }

    inline void append_u64(std::vector<std::byte>& bytes, const std::uint64_t value)
    {
        append_big_endian(bytes, value);
    }
}
