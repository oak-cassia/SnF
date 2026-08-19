#pragma once

#include <memory>
#include <optional>
#include <utility>

namespace snf::runtime
{
    // A domain payload carried from one actor's Binding to the target actor's
    // Binding. The runtime never inspects it: only the Binding that owns the type
    // restores it, which is what keeps actor routing free of domain types while
    // still refusing a submission the target binding did not build.
    //
    // Move-only, like ActorSubmission. A tell is delivered at most once, so a
    // carrier that has been taken from is empty rather than copyable.
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

        // Moves the value out when the carrier holds exactly this type, leaving the
        // carrier empty. std::nullopt means the carrier is empty or holds another
        // type: the target Binding refuses the tell instead of guessing, so a
        // mismatched payload cannot reach an actor as the wrong command.
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
