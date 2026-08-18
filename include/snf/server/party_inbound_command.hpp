#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/party_command.hpp"
#include "snf/server/party_id.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    enum class PartyReplyKind
    {
        Joined,
        Left,
    };

    struct PartyReplyContext
    {
        snf::net::ConnectionId connection;
        std::uint32_t request_id{0};
        PartyReplyKind kind{PartyReplyKind::Joined};
    };

    struct PartyInboundCommand
    {
        PartyId party;
        snf::net::ConnectionId connection;
        PartyCommand command;
        std::optional<PartyReplyContext> reply;
    };
}
