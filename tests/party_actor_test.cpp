#include "snf/game/party_actor.hpp"

#include <cassert>
#include <vector>

namespace
{
    void test_party_actor_orders_members_and_enforces_epoch_and_capacity()
    {
        snf::server::PartyActor actor{snf::server::PartyId{.value = 9}, snf::server::PartyActorConfig{.max_members = 2}};
        const snf::server::PlayerId first{.value = 20};
        const snf::server::PlayerId second{.value = 10};
        const snf::server::PlayerId third{.value = 30};

        auto result = actor.handle(snf::server::JoinPartyCommand{
            .player = first,
            .membership_epoch = 2,
        });
        assert(result.status == snf::server::PartyCommandStatus::Applied);
        result = actor.handle(snf::server::JoinPartyCommand{
            .player = second,
            .membership_epoch = 1,
        });
        assert(result.members == std::vector<snf::server::PlayerId>({second, first}));

        result = actor.handle(snf::server::JoinPartyCommand{
            .player = first,
            .membership_epoch = 2,
        });
        assert(result.status == snf::server::PartyCommandStatus::AlreadyMember);
        result = actor.handle(snf::server::JoinPartyCommand{
            .player = first,
            .membership_epoch = 1,
        });
        assert(result.status == snf::server::PartyCommandStatus::StaleMembership);
        assert(result.membership_epoch == 2);

        result = actor.handle(snf::server::JoinPartyCommand{
            .player = third,
            .membership_epoch = 1,
        });
        assert(result.status == snf::server::PartyCommandStatus::PartyFull);
        assert(actor.memberCount() == 2);

        result = actor.handle(snf::server::LeavePartyCommand{
            .player = first,
            .membership_epoch = 3,
        });
        assert(result.status == snf::server::PartyCommandStatus::StaleMembership);
        assert(actor.memberCount() == 2);
        result = actor.handle(snf::server::LeavePartyCommand{
            .player = first,
            .membership_epoch = 2,
        });
        assert(result.status == snf::server::PartyCommandStatus::Applied);
        assert(result.members == std::vector<snf::server::PlayerId>{second});
        result = actor.handle(snf::server::LeavePartyCommand{
            .player = second,
            .membership_epoch = 1,
        });
        assert(result.status == snf::server::PartyCommandStatus::Applied);
        assert(result.members.empty());
    }
}

void run_party_actor_tests()
{
    test_party_actor_orders_members_and_enforces_epoch_and_capacity();
}
