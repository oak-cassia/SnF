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

    void test_registers_an_additional_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        const bool registered = dispatcher.registerHandler(
            snf::protocol::MessageType::Pong,
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

        const bool registered = dispatcher.registerHandler(
            snf::protocol::MessageType::Ping,
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
        assert(dispatcher.registerHandler(
            snf::protocol::MessageType::Pong,
            [](snf::protocol::Frame) -> std::optional<snf::server::PlayerCommand>
            { return std::nullopt; }));

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
    test_registers_an_additional_handler();
    test_rejects_a_duplicate_handler();
    test_reports_invalid_payload_from_a_registered_handler();
}
