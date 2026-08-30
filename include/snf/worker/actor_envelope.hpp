#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <typeinfo>
#include <utility>

namespace snf::worker
{
    template <typename T>
    struct ActorPayloadTraits;

    class ActorPayloadConcept
    {
    public:
        virtual ~ActorPayloadConcept() = default;
        [[nodiscard]] virtual std::uint32_t tag() const noexcept = 0;
        [[nodiscard]] virtual std::uint64_t chargedBytes() const noexcept = 0;
        [[nodiscard]] virtual const std::type_info& typeInfo() const noexcept = 0;
        [[nodiscard]] virtual void* rawPointer() noexcept = 0;
        [[nodiscard]] virtual const void* rawPointer() const noexcept = 0;
    };

    template <typename T>
    class ActorPayloadModel final : public ActorPayloadConcept
    {
    public:
        explicit ActorPayloadModel(T&& val, const std::uint32_t tag, const std::uint64_t charge) noexcept(std::is_nothrow_move_constructible_v<T>)
            : _value(std::move(val))
            , _tag(tag)
            , _charge(charge)
        {
        }

        [[nodiscard]] std::uint32_t tag() const noexcept override
        {
            return _tag;
        }

        [[nodiscard]] std::uint64_t chargedBytes() const noexcept override
        {
            return _charge;
        }

        [[nodiscard]] const std::type_info& typeInfo() const noexcept override
        {
            return typeid(T);
        }

        [[nodiscard]] void* rawPointer() noexcept override
        {
            return &_value;
        }

        [[nodiscard]] const void* rawPointer() const noexcept override
        {
            return &_value;
        }

        [[nodiscard]] T& value() noexcept
        {
            return _value;
        }

    private:
        T _value;
        std::uint32_t _tag;
        std::uint64_t _charge;
    };

    class ActorEnvelope final
    {
    public:
        ActorEnvelope() noexcept = default;
        ~ActorEnvelope() = default;

        ActorEnvelope(const ActorEnvelope&) = delete;
        ActorEnvelope& operator=(const ActorEnvelope&) = delete;

        ActorEnvelope(ActorEnvelope&&) noexcept = default;
        ActorEnvelope& operator=(ActorEnvelope&&) noexcept = default;

        [[nodiscard]] bool empty() const noexcept
        {
            return _concept == nullptr;
        }

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return _concept != nullptr;
        }

        [[nodiscard]] std::uint32_t tag() const noexcept
        {
            return _concept ? _concept->tag() : 0;
        }

        [[nodiscard]] std::uint64_t chargedBytes() const noexcept
        {
            return _concept ? _concept->chargedBytes() : 0;
        }

        template <typename T>
        [[nodiscard]] bool is() const noexcept
        {
            if (!_concept)
            {
                return false;
            }
            return _concept->tag() == ActorPayloadTraits<std::decay_t<T>>::TAG &&
                   _concept->typeInfo() == typeid(std::decay_t<T>);
        }

        template <typename T>
        [[nodiscard]] const T& get() const
        {
            if (!is<T>())
            {
                throw std::bad_cast{};
            }
            return *static_cast<const T*>(_concept->rawPointer());
        }

        template <typename T>
        [[nodiscard]] T& get()
        {
            if (!is<T>())
            {
                throw std::bad_cast{};
            }
            return *static_cast<T*>(_concept->rawPointer());
        }

        template <typename T>
        [[nodiscard]] T take()
        {
            if (!is<T>())
            {
                throw std::bad_cast{};
            }
            auto* model = static_cast<ActorPayloadModel<std::decay_t<T>>*>(_concept.get());
            T val = std::move(model->value());
            _concept.reset();
            return val;
        }

        void reset() noexcept
        {
            _concept.reset();
        }

    private:
        template <typename... RegisteredTypes>
        friend class ActorPayloadRegistry;

        explicit ActorEnvelope(std::unique_ptr<ActorPayloadConcept> concept_ptr) noexcept
            : _concept(std::move(concept_ptr))
        {
        }

        std::unique_ptr<ActorPayloadConcept> _concept{nullptr};
    };

    static_assert(std::is_nothrow_move_constructible_v<ActorEnvelope>);
    static_assert(!std::is_copy_constructible_v<ActorEnvelope>);
    static_assert(!std::is_copy_assignable_v<ActorEnvelope>);

    template <typename... Ts>
    class ActorPayloadRegistry final
    {
    private:
        static consteval bool hasUniqueTags()
        {
            constexpr std::size_t N = sizeof...(Ts);
            if constexpr (N <= 1)
            {
                return true;
            }
            else
            {
                constexpr std::array<std::uint32_t, N> tags = {ActorPayloadTraits<Ts>::TAG...};
                for (std::size_t i = 0; i < N; ++i)
                {
                    if (tags[i] == 0)
                    {
                        return false;
                    }
                    for (std::size_t j = i + 1; j < N; ++j)
                    {
                        if (tags[i] == tags[j])
                        {
                            return false;
                        }
                    }
                }
                return true;
            }
        }

        static_assert(hasUniqueTags(), "ActorPayloadRegistry: TAG values must be non-zero and unique!");

    public:
        template <typename T>
        static constexpr bool isRegistered()
        {
            return (std::is_same_v<std::decay_t<T>, Ts> || ...);
        }

        template <typename T>
        [[nodiscard]] static ActorEnvelope create(T&& payload)
        {
            using DecayedT = std::decay_t<T>;
            static_assert(isRegistered<DecayedT>(), "Payload type T is not registered in this ActorPayloadRegistry!");
            constexpr std::uint32_t tag = ActorPayloadTraits<DecayedT>::TAG;
            const std::uint64_t charge = ActorPayloadTraits<DecayedT>::calculateCharge(payload);
            auto model = std::make_unique<ActorPayloadModel<DecayedT>>(std::forward<T>(payload), tag, charge);
            return ActorEnvelope{std::move(model)};
        }
    };
}
