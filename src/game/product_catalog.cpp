#include "snf/game/product_catalog.hpp"

namespace snf::server
{
    std::optional<ProductDefinition> ProductCatalog::find(const ProductId product) const noexcept
    {
        if (product == BASIC_PRODUCT)
        {
            return ProductDefinition{
                .product = BASIC_PRODUCT,
                .price = BASIC_PRODUCT_PRICE,
                .reward = AddPurchasedItemCountReward{.item_count = BASIC_PRODUCT_GRANT_COUNT},
            };
        }
        if (product == ARCANE_BOLT_PRODUCT)
        {
            return ProductDefinition{
                .product = ARCANE_BOLT_PRODUCT,
                .price = ARCANE_BOLT_PRODUCT_PRICE,
                .reward = AddOwnedSkillReward{.skill_id = ARCANE_BOLT_SKILL_ID},
            };
        }
        return std::nullopt;
    }

    const ProductCatalog& productCatalog() noexcept
    {
        static const ProductCatalog catalog;
        return catalog;
    }

    std::optional<ProductDefinition> findProduct(const ProductId product) noexcept
    {
        return productCatalog().find(product);
    }
}
