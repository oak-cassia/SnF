#include "snf/server/zone_actor.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <variant>

namespace snf::server
{
    ZoneActor::ZoneActor(const ZoneId zone, const ZoneActorConfig config)
        : _zone(zone)
        , _aoi_radius(config.aoi_radius)
        , _tick_interval(config.tick_interval)
    {
        if (_zone.value == 0)
        {
            throw std::invalid_argument{"ZoneId must be non-zero"};
        }
        if (_aoi_radius < 0)
        {
            throw std::invalid_argument{"Zone AOI radius cannot be negative"};
        }
        if (_tick_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Zone tick interval must be positive"};
        }
    }

    ZoneId ZoneActor::id() const noexcept
    {
        return _zone;
    }

    std::size_t ZoneActor::playerCount() const noexcept
    {
        return _players.size();
    }

    std::uint64_t ZoneActor::lastTick() const noexcept
    {
        return _last_tick;
    }

    std::chrono::milliseconds ZoneActor::tickInterval() const noexcept
    {
        return _tick_interval;
    }

    std::optional<ZonePosition> ZoneActor::positionOf(const PlayerId player) const
    {
        const auto iterator = _players.find(player);
        if (iterator == _players.end())
        {
            return std::nullopt;
        }

        return iterator->second.position;
    }

    std::vector<PlayerId> ZoneActor::visiblePlayers(const PlayerId player) const
    {
        const auto subject = _players.find(player);
        if (subject == _players.end())
        {
            return {};
        }

        std::vector<PlayerId> visible;
        visible.reserve(_players.size());
        for (const auto& [candidate, entity] : _players)
        {
            if (candidate != player && isVisible(subject->second.position, entity.position))
            {
                visible.push_back(candidate);
            }
        }

        std::sort(visible.begin(), visible.end(), [](const PlayerId left, const PlayerId right) { return left.value < right.value; });
        return visible;
    }

    ZoneResult ZoneActor::handle(const ZoneCommand& command)
    {
        return std::visit([this](const auto& value) { return handleCommand(value); }, command);
    }

    ZoneResult ZoneActor::handleCommand(const EnterZoneCommand& command)
    {
        const auto existing = _players.find(command.player);
        if (existing != _players.end())
        {
            if (command.route_epoch < existing->second.route_epoch)
            {
                return ZoneResult{
                    .status = ZoneCommandStatus::StaleRoute,
                    .player = command.player,
                    .position = existing->second.position,
                    .route_epoch = existing->second.route_epoch,
                    .tick = _last_tick,
                    .visible_players = visiblePlayers(command.player),
                };
            }
            if (command.route_epoch == existing->second.route_epoch)
            {
                return ZoneResult{
                    .status = ZoneCommandStatus::AlreadyPresent,
                    .player = command.player,
                    .position = existing->second.position,
                    .route_epoch = existing->second.route_epoch,
                    .tick = _last_tick,
                    .visible_players = visiblePlayers(command.player),
                };
            }
        }

        const bool was_empty = _players.empty();
        _players.insert_or_assign(command.player,
                                  Entity{
                                      .position = command.position,
                                      .route_epoch = command.route_epoch,
                                  });

        std::vector<FollowUpAction> follow_ups;
        if (was_empty && !_players.empty())
        {
            follow_ups.push_back(ScheduleTimer{.delay = _tick_interval});
        }

        return ZoneResult{
            .status = ZoneCommandStatus::Applied,
            .player = command.player,
            .position = command.position,
            .route_epoch = command.route_epoch,
            .tick = _last_tick,
            .visible_players = visiblePlayers(command.player),
            .follow_ups = std::move(follow_ups),
        };
    }

    ZoneResult ZoneActor::handleCommand(const LeaveZoneCommand& command)
    {
        const auto existing = _players.find(command.player);
        if (existing == _players.end())
        {
            return ZoneResult{
                .status = ZoneCommandStatus::PlayerMissing,
                .player = command.player,
                .position = std::nullopt,
                .route_epoch = command.route_epoch,
                .tick = _last_tick,
                .visible_players = {},
            };
        }
        if (command.route_epoch != existing->second.route_epoch)
        {
            return ZoneResult{
                .status = ZoneCommandStatus::StaleRoute,
                .player = command.player,
                .position = existing->second.position,
                .route_epoch = existing->second.route_epoch,
                .tick = _last_tick,
                .visible_players = visiblePlayers(command.player),
            };
        }

        const ZonePosition position = existing->second.position;
        _players.erase(existing);
        return ZoneResult{
            .status = ZoneCommandStatus::Applied,
            .player = command.player,
            .position = position,
            .route_epoch = command.route_epoch,
            .tick = _last_tick,
            .visible_players = {},
        };
    }

    ZoneResult ZoneActor::handleCommand(const MoveInZoneCommand& command)
    {
        const auto existing = _players.find(command.player);
        if (existing == _players.end())
        {
            return ZoneResult{
                .status = ZoneCommandStatus::PlayerMissing,
                .player = command.player,
                .position = command.position,
                .route_epoch = command.route_epoch,
                .tick = _last_tick,
                .visible_players = {},
            };
        }
        if (command.route_epoch != existing->second.route_epoch)
        {
            return ZoneResult{
                .status = ZoneCommandStatus::StaleRoute,
                .player = command.player,
                .position = existing->second.position,
                .route_epoch = existing->second.route_epoch,
                .tick = _last_tick,
                .visible_players = visiblePlayers(command.player),
            };
        }

        existing->second.position = command.position;
        return ZoneResult{
            .status = ZoneCommandStatus::Applied,
            .player = command.player,
            .position = command.position,
            .route_epoch = command.route_epoch,
            .tick = _last_tick,
            .visible_players = visiblePlayers(command.player),
        };
    }

    ZoneResult ZoneActor::handleCommand(const ZoneSimulationTick&)
    {
        ++_last_tick;
        std::vector<FollowUpAction> follow_ups;
        if (!_players.empty())
        {
            follow_ups.push_back(ScheduleTimer{.delay = _tick_interval});
        }

        return ZoneResult{
            .status = ZoneCommandStatus::Applied,
            .player = std::nullopt,
            .position = std::nullopt,
            .route_epoch = 0,
            .tick = _last_tick,
            .visible_players = {},
            .follow_ups = std::move(follow_ups),
        };
    }

    bool ZoneActor::isVisible(const ZonePosition left, const ZonePosition right) const noexcept
    {
        const std::int64_t x = static_cast<std::int64_t>(left.x) - right.x;
        const std::int64_t y = static_cast<std::int64_t>(left.y) - right.y;
        const std::int64_t radius = _aoi_radius;
        if (std::abs(x) > radius || std::abs(y) > radius)
        {
            return false;
        }

        return x * x + y * y <= radius * radius;
    }
}
