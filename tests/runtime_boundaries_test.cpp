#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/outbound_sink.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <stop_token>
#include <sys/eventfd.h>
#include <unistd.h>
#include <variant>

namespace
{
    using namespace std::chrono_literals;

    snf::net::UniqueFileDescriptor make_eventfd()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
    }

    std::uint64_t read_wakeup_count(const int descriptor)
    {
        std::uint64_t wakeup_count = 0;
        assert(::read(descriptor, &wakeup_count, sizeof(wakeup_count)) ==
               static_cast<ssize_t>(sizeof(wakeup_count)));
        return wakeup_count;
    }

    void test_outbound_sink_hides_queue_and_wakeup()
    {
        snf::runtime::BoundedQueue<snf::server::OutboundAction> actions{2};
        const auto event = make_eventfd();
        snf::server::EventFdOutboundSink sink{actions, event.getDescriptor()};

        assert(sink.publish(
            snf::server::SendFrame{
                .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 7},
                .frame =
                    snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Pong,
                        .request_id = 9,
                        .payload = {},
                    },
            },
            {}));

        assert(read_wakeup_count(event.getDescriptor()) == 1);
        const auto action = actions.tryPop();
        assert(action.has_value());
        const auto* send = std::get_if<snf::server::SendFrame>(&*action);
        assert(send != nullptr);
        assert(send->connection.generation == 7);
        assert(send->frame.request_id == 9);

        actions.cancel();
        assert(!sink.publish(
            snf::server::CloseConnection{
                .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 7},
                .reason = snf::server::CloseReason::ProtocolError,
            },
            {}));
    }

    void test_runtime_completion_is_authoritative_and_independent_from_outbound_capacity()
    {
        snf::runtime::BoundedQueue<snf::server::OutboundAction> full_outbound{1};
        assert(full_outbound.tryPush(snf::server::CloseConnection{
            .connection = snf::net::ConnectionId{.descriptor = 1, .generation = 1},
            .reason = snf::server::CloseReason::ProtocolError,
        }));

        const auto event = make_eventfd();
        constexpr std::uint64_t required =
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic);
        snf::runtime::RuntimeCompletionCoordinator completion{required, event.getDescriptor()};

        completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        assert(completion.allRequiredRuntimesDrained());
        assert(!completion.anyRuntimeFailed());

        completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        assert(completion.anyRuntimeFailed());
        assert(read_wakeup_count(event.getDescriptor()) == 3);
        assert(full_outbound.size() == 1);
    }

    void test_outbound_sink_wait_is_runtime_cancelable_without_canceling_shared_queue()
    {
        snf::runtime::BoundedQueue<snf::server::OutboundAction> actions{1};
        const auto event = make_eventfd();
        snf::server::EventFdOutboundSink sink{actions, event.getDescriptor()};

        assert(sink.publish(
            snf::server::SendFrame{
                .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 7},
                .frame =
                    snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Pong,
                        .request_id = 1,
                        .payload = {},
                    },
            },
            {}));
        assert(read_wakeup_count(event.getDescriptor()) == 1);

        std::stop_source publish_stop;
        auto blocked_publish = std::async(
            std::launch::async,
            [&sink, stop_token = publish_stop.get_token()]
            {
                return sink.publish(
                    snf::server::SendFrame{
                        .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 7},
                        .frame =
                            snf::protocol::Frame{
                                .type = snf::protocol::MessageType::Pong,
                                .request_id = 2,
                                .payload = {},
                            },
                    },
                    stop_token);
            });

        assert(blocked_publish.wait_for(100ms) == std::future_status::timeout);
        publish_stop.request_stop();
        const bool stopped = blocked_publish.wait_for(1s) == std::future_status::ready;
        if (!stopped)
        {
            actions.cancel();
        }
        assert(!blocked_publish.get());
        assert(stopped);
        assert(actions.size() == 1);

        assert(actions.tryPop().has_value());
        assert(sink.publish(
            snf::server::CloseConnection{
                .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 7},
                .reason = snf::server::CloseReason::ProtocolError,
            },
            {}));
        assert(read_wakeup_count(event.getDescriptor()) == 1);
    }

    void test_rejects_invalid_boundary_configuration()
    {
        snf::runtime::BoundedQueue<snf::server::OutboundAction> actions{1};

        bool invalid_outbound_descriptor_rejected = false;
        try
        {
            [[maybe_unused]] snf::server::EventFdOutboundSink sink{actions, -1};
        }
        catch (const std::invalid_argument&)
        {
            invalid_outbound_descriptor_rejected = true;
        }
        assert(invalid_outbound_descriptor_rejected);

        const auto event = make_eventfd();
        bool empty_required_mask_rejected = false;
        try
        {
            [[maybe_unused]] snf::runtime::RuntimeCompletionCoordinator completion{
                0, event.getDescriptor()};
        }
        catch (const std::invalid_argument&)
        {
            empty_required_mask_rejected = true;
        }
        assert(empty_required_mask_rejected);
    }
}

void run_runtime_boundary_tests()
{
    test_outbound_sink_hides_queue_and_wakeup();
    test_runtime_completion_is_authoritative_and_independent_from_outbound_capacity();
    test_outbound_sink_wait_is_runtime_cancelable_without_canceling_shared_queue();
    test_rejects_invalid_boundary_configuration();
}
