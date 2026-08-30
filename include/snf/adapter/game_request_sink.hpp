#pragma once

#include "snf/worker/request_sink.hpp"
#include "snf/worker/worker.hpp"

namespace snf::adapter
{
    class GameRequestSink final : public snf::worker::RequestSink
    {
    public:
        GameRequestSink() = default;
        explicit GameRequestSink(snf::worker::Worker& worker) noexcept
            : _worker(&worker)
        {
        }

        void setWorker(snf::worker::Worker& worker) noexcept
        {
            _worker = &worker;
        }

        [[nodiscard]] snf::worker::RequestPostResult tryPost(
            snf::worker::ConnectionRef connection,
            snf::protocol::Frame&& frame
        ) override;

    private:
        snf::worker::Worker* _worker{nullptr};
    };
}
