#pragma once

#include "snf/game/player.hpp"
#include "snf/game/player_record.hpp"
#include "snf/worker/actor.hpp"

#include <chrono>

namespace snf::adapter
{
    class PlayerActorAdapter final : public snf::worker::ActorInstance
    {
    public:
        explicit PlayerActorAdapter(std::optional<snf::server::PlayerId> player_id = std::nullopt)
            : _player(player_id)
        {
        }

        // Built from persisted state after an activation load.
        PlayerActorAdapter(const snf::server::PlayerId player_id, const snf::server::PlayerRecord& record)
            : _player(player_id)
        {
            _player.restore(record);
        }

        [[nodiscard]] snf::server::Player& player() noexcept
        {
            return _player;
        }

        [[nodiscard]] const snf::server::Player& player() const noexcept
        {
            return _player;
        }

        void setSaveInterval(const std::chrono::milliseconds interval) noexcept
        {
            _save_interval = interval;
        }

        [[nodiscard]] std::uint64_t committedSaves() const noexcept
        {
            return _committed_saves;
        }

        [[nodiscard]] std::uint64_t unknownCommits() const noexcept
        {
            return _unknown_commits;
        }

        [[nodiscard]] snf::worker::TurnResult dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context) override;

        // Called from the save continuation once the database has answered. Public
        // because the continuation is a free coroutine, not a member.
        void onSaveCompleted(const snf::worker::DbResult& result, snf::server::PlayerStateComponentMask cleared);

        // The connection this player is authenticated on, if any.
        [[nodiscard]] std::optional<snf::worker::ConnectionRef> boundConnection() const noexcept
        {
            return _bound_connection;
        }

        [[nodiscard]] std::uint64_t authenticationConflicts() const noexcept
        {
            return _authentication_conflicts;
        }

    private:
        void scheduleSaveIfDirty(snf::worker::EffectBatch& effects, std::chrono::steady_clock::time_point now);
        [[nodiscard]] std::optional<snf::worker::EffectBatch> rejectConflictingAuthentication(
            const std::optional<snf::worker::ConnectionRef>& connection
        );

        snf::server::Player _player;
        // player -> connection. The sink owns connection -> player.
        std::optional<snf::worker::ConnectionRef> _bound_connection{std::nullopt};
        std::uint64_t _authentication_conflicts{0};
        std::chrono::milliseconds _save_interval{std::chrono::seconds{5}};
        // Only one save timer may be outstanding. The actor is Suspended for the
        // duration of the await, so a second save cannot overlap the first.
        bool _save_scheduled{false};
        std::uint64_t _committed_saves{0};
        std::uint64_t _unknown_commits{0};
    };
}
