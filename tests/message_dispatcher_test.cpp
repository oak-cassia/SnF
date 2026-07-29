#include "snf/server/message_dispatcher.hpp"

#include <cassert>
#include <cstddef>
#include <optional>
#include <utility>
#include <variant>

namespace
{
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
    test_registers_an_additional_handler();
    test_rejects_a_duplicate_handler();
    test_reports_invalid_payload_from_a_registered_handler();
}
