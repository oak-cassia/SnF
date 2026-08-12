#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>

namespace snf::server
{
    struct PurchaseIdempotencyKey
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const PurchaseIdempotencyKey&) const noexcept = default;
    };

    struct ProductId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const ProductId&) const noexcept = default;
    };

    struct PurchaseRequest
    {
        PlayerId player;
        PurchaseIdempotencyKey idempotency_key;
        ProductId product;
    };

    enum class PurchaseStatus : std::uint8_t
    {
        Committed = 0,
        InsufficientFunds = 1,
        ProductNotFound = 2,
        InventoryCapacityExceeded = 3,
        IdempotencyConflict = 4,
        IdempotencyCapacityExceeded = 5,
        Unavailable = 6,
    };

    struct PurchaseTransactionResult
    {
        PurchaseStatus status{PurchaseStatus::Unavailable};
        PlayerId player;
        PurchaseIdempotencyKey idempotency_key;
        ProductId product;
        std::uint64_t currency_balance{0};
        std::uint64_t purchased_item_count{0};
        bool replayed{false};

        [[nodiscard]] bool committed() const noexcept
        {
            return status == PurchaseStatus::Committed;
        }
    };

    inline constexpr ProductId BASIC_PRODUCT{.value = 1};
    inline constexpr std::uint64_t BASIC_PRODUCT_PRICE = 100;
    inline constexpr std::uint64_t BASIC_PRODUCT_GRANT_COUNT = 1;
    inline constexpr std::uint64_t INITIAL_CURRENCY_BALANCE = 1000;
}
