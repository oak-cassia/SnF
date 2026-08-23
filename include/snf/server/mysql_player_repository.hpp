#pragma once

#include "snf/server/player_repository.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace snf::server
{
    struct MySqlPlayerRepositoryConfig
    {
        std::string host{"127.0.0.1"};
        std::uint16_t port{3306};
        std::string user;
        std::string password;
        std::string database{"snf"};
        std::size_t worker_count{2};
        std::size_t queue_capacity{4096};
        std::chrono::seconds connect_timeout{5};
        std::chrono::seconds read_timeout{5};
        std::chrono::seconds write_timeout{5};
    };

    class MySqlPlayerRepository final : public PlayerRepository, public PlayerRepositoryDiagnostics
    {
    public:
        explicit MySqlPlayerRepository(MySqlPlayerRepositoryConfig config);
        ~MySqlPlayerRepository();

        MySqlPlayerRepository(const MySqlPlayerRepository&) = delete;
        MySqlPlayerRepository& operator=(const MySqlPlayerRepository&) = delete;
        MySqlPlayerRepository(MySqlPlayerRepository&&) = delete;
        MySqlPlayerRepository& operator=(MySqlPlayerRepository&&) = delete;

        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;

        void close() noexcept;
        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const override;
        [[nodiscard]] PlayerRepositoryStats stats() const override;

    private:
        class Impl;
        std::unique_ptr<Impl> _impl;
    };
}
