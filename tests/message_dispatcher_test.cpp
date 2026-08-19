#include "snf/server/message_dispatcher.hpp"

#include <cassert>
#include <cstddef>
#include <optional>
#include <utility>
#include <variant>

namespace
{
    std::vector<std::byte> player_id_payload(const std::uint64_t value)
    {
        std::vector<std::byte> payload(8);
        std::uint64_t remaining = value;
        for (std::size_t index = payload.size(); index > 0; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
            remaining >>= 8U;
        }
        return payload;
    }

    std::vector<std::byte> purchase_payload(const std::uint64_t key, const std::uint32_t product)
    {
        std::vector<std::byte> payload(12);
        std::uint64_t remaining_key = key;
        for (std::size_t index = 8; index > 0; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining_key & 0xFFU);
            remaining_key >>= 8U;
        }
        std::uint32_t remaining_product = product;
        for (std::size_t index = payload.size(); index > 8; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining_product & 0xFFU);
            remaining_product >>= 8U;
        }
        return payload;
    }

    void test_dispatches_ping_to_the_registered_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = {std::byte{0xAA}, std::byte{0xBB}},
        };

        const auto result = dispatcher.dispatch(request);

        assert(result.handled());
        assert(result.command.has_value());
        const auto* command = std::get_if<snf::server::PingCommand>(&*result.command);
        assert(command != nullptr);
        assert(command->request_id == request.request_id);
        assert(command->payload == request.payload);
    }

    void test_reports_a_missing_handler()
    {
        const snf::server::MessageDispatcher dispatcher;
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        };

        const auto result = dispatcher.dispatch(request);

        assert(!result.handled());
        assert(result.status == snf::server::DispatchStatus::HandlerNotFound);
        assert(!result.command.has_value());
    }

    void test_dispatches_a_valid_persistent_player_authentication()
    {
        const snf::server::MessageDispatcher dispatcher;
        const auto result = dispatcher.dispatch(snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = 8,
            .payload = player_id_payload(77),
        });

        assert(result.handled());
        const auto* authenticate = std::get_if<snf::server::AuthenticateCommand>(&*result.command);
        assert(authenticate != nullptr);
        assert(authenticate->request_id == 8);
        assert(authenticate->player == snf::server::PlayerId{.value = 77});

        const auto invalid = dispatcher.dispatch(snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = 9,
            .payload = player_id_payload(0),
        });
        assert(invalid.status == snf::server::DispatchStatus::InvalidPayload);
    }

    void test_dispatches_and_validates_a_purchase()
    {
        const snf::server::MessageDispatcher dispatcher;
        const auto result = dispatcher.dispatch(snf::protocol::Frame{
            .type = snf::protocol::MessageType::Purchase,
            .request_id = 10,
            .payload = purchase_payload(0x0102030405060708ULL, 0x11223344U),
        });

        assert(result.handled());
        const auto* purchase = std::get_if<snf::server::PurchaseCommand>(&*result.command);
        assert(purchase != nullptr);
        assert(purchase->request_id == 10);
        assert(purchase->idempotency_key.value == 0x0102030405060708ULL);
        assert(purchase->product.value == 0x11223344U);

        for (const auto& payload : {purchase_payload(0, 1), purchase_payload(1, 0), std::vector<std::byte>(11)})
        {
            const auto invalid = dispatcher.dispatch(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Purchase,
                .request_id = 11,
                .payload = payload,
            });
            assert(invalid.status == snf::server::DispatchStatus::InvalidPayload);
        }
    }

    void test_registers_an_additional_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        const bool registered = dispatcher.registerHandler(snf::protocol::MessageType::Pong,
                                                           [](snf::protocol::Frame request) -> snf::server::PlayerCommand
                                                           {
                                                               return snf::server::PingCommand{
                                                                   .request_id = request.request_id,
                                                                   .payload = std::move(request.payload),
                                                               };
                                                           });

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        };
        const auto result = dispatcher.dispatch(request);

        assert(registered);
        assert(result.handled());
        assert(result.command.has_value());
        assert(std::holds_alternative<snf::server::PingCommand>(*result.command));
    }

    void test_rejects_a_duplicate_handler()
    {
        snf::server::MessageDispatcher dispatcher;

        const bool registered = dispatcher.registerHandler(snf::protocol::MessageType::Ping,
                                                           [](snf::protocol::Frame request) -> snf::server::PlayerCommand
                                                           {
                                                               return snf::server::PingCommand{
                                                                   .request_id = request.request_id,
                                                                   .payload = std::move(request.payload),
                                                               };
                                                           });

        assert(!registered);
    }

    void test_reports_invalid_payload_from_a_registered_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        assert(dispatcher.registerHandler(snf::protocol::MessageType::Pong, [](snf::protocol::Frame) -> std::optional<snf::server::PlayerCommand> { return std::nullopt; }));

        const auto result = dispatcher.dispatch(snf::protocol::Frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        });

        assert(!result.handled());
        assert(result.status == snf::server::DispatchStatus::InvalidPayload);
        assert(!result.command.has_value());
    }
}

void run_message_dispatcher_tests()
{
    test_dispatches_ping_to_the_registered_handler();
    test_reports_a_missing_handler();
    test_dispatches_a_valid_persistent_player_authentication();
    test_dispatches_and_validates_a_purchase();
    test_registers_an_additional_handler();
    test_rejects_a_duplicate_handler();
    test_reports_invalid_payload_from_a_registered_handler();
}
