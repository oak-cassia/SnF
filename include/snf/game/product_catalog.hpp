#pragma once

#include "snf/game/purchase.hpp"
#include "snf/game/skill_id.hpp"

#include <cstdint>
#include <optional>
#include <variant>

namespace snf::server
{
    struct AddPurchasedItemCountReward
    {
        std::uint64_t item_count{0};
    };

    struct AddOwnedSkillReward
    {
        SkillId skill_id{};
    };

    using ProductReward = std::variant<AddPurchasedItemCountReward, AddOwnedSkillReward>;

    struct ProductDefinition
    {
        ProductId product;
        std::uint64_t price{0};
        ProductReward reward{};
    };

    class ProductCatalog final
    {
    public:
        [[nodiscard]] std::optional<ProductDefinition> find(ProductId product) const noexcept;
    };

    [[nodiscard]] const ProductCatalog& productCatalog() noexcept;
    [[nodiscard]] std::optional<ProductDefinition> findProduct(ProductId product) noexcept;
}
