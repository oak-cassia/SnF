#pragma once

#include <memory>
#include <optional>
#include <utility>

namespace snf::runtime
{
    class TellPayload final
    {
    public:
        TellPayload() = default;

        TellPayload(const TellPayload&) = delete;
        TellPayload& operator=(const TellPayload&) = delete;
        TellPayload(TellPayload&&) noexcept = default;
        TellPayload& operator=(TellPayload&&) noexcept = default;

        template <typename Payload> [[nodiscard]] static TellPayload of(Payload payload)
        {
            TellPayload carrier;
            carrier._storage = std::make_unique<TypedStorage<Payload>>(std::move(payload));
            return carrier;
        }

        [[nodiscard]] bool empty() const noexcept
        {
            return _storage == nullptr;
        }

        template <typename Payload> [[nodiscard]] std::optional<Payload> take()
        {
            auto* typed = dynamic_cast<TypedStorage<Payload>*>(_storage.get());
            if (typed == nullptr)
            {
                return std::nullopt;
            }

            std::optional<Payload> value{std::move(typed->value)};
            _storage.reset();
            return value;
        }

    private:
        class Storage
        {
        public:
            virtual ~Storage() = default;
        };

        template <typename Payload> class TypedStorage final : public Storage
        {
        public:
            explicit TypedStorage(Payload payload)
                : value(std::move(payload))
            {
            }

            Payload value;
        };

        std::unique_ptr<Storage> _storage;
    };
}
