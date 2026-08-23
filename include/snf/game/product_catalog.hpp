#pragma once

#include "snf/game/purchase.hpp"

#include <optional>

namespace snf::server
{
    struct ProductDefinition
    {
        ProductId product;
        std::uint64_t price{0};
        std::uint64_t grant_count{0};
    };

    class ProductCatalog final
    {
    public:
        [[nodiscard]] std::optional<ProductDefinition> find(ProductId product) const noexcept;
    };

    [[nodiscard]] const ProductCatalog& productCatalog() noexcept;
    [[nodiscard]] std::optional<ProductDefinition> findProduct(ProductId product) noexcept;
}
