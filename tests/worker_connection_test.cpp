#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/connection_table.hpp"
#include "snf/worker/connection_work_queue.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poll_registration.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <sys/socket.h>
#include <utility>
#include <vector>

namespace
{
    using namespace snf::worker;
    using snf::protocol::Frame;
    using snf::protocol::MessageType;

    struct SocketPair
    {
        snf::net::UniqueFileDescriptor left;
        snf::net::UniqueFileDescriptor right;
    };

    [[nodiscard]] SocketPair makeSocketPair()
    {
        int descriptors[2]{};
        assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors) == 0);
        return SocketPair{snf::net::UniqueFileDescriptor{descriptors[0]}, snf::net::UniqueFileDescriptor{descriptors[1]}};
    }

    [[nodiscard]] Frame makeFrame(const std::size_t payload_size, const MessageType type = MessageType::Ping)
    {
        return Frame{
            .type = type,
            .request_id = 77,
            .payload = std::vector<std::byte>(payload_size, std::byte{0x5A}),
        };
    }

    void test_wire_size_contract()
    {
        static_assert(snf::protocol::MAX_BODY_SIZE == 65'536);
        static_assert(snf::protocol::MAX_PAYLOAD_SIZE == 65'530);
        static_assert(snf::protocol::MAX_FRAME_SIZE == 65'540);

        const auto encoded = snf::protocol::encode_frame(makeFrame(snf::protocol::MAX_PAYLOAD_SIZE));
        assert(encoded.size() == snf::protocol::MAX_FRAME_SIZE);
    }

    void test_frame_decoder_reports_logical_buffer_size_and_resets()
    {
        const Frame frame = makeFrame(32);
        const auto encoded = snf::protocol::encode_frame(frame);
        snf::protocol::FrameDecoder decoder;

        const auto first = decoder.append(std::span<const std::byte>{encoded.data(), 3});
        assert(first.ok());
        assert(first.frames.empty());
        assert(decoder.bufferedByteCount() == 3);

        const auto second = decoder.append(std::span<const std::byte>{encoded.data() + 3, encoded.size() - 3});
        assert(second.ok());
        assert(second.frames.size() == 1);
        assert(second.frames.front() == frame);
        assert(decoder.bufferedByteCount() == 0);

        decoder.push(std::span<const std::byte>{encoded.data(), 5});
        assert(decoder.bufferedByteCount() == 5);
        decoder.reset();
        assert(decoder.bufferedByteCount() == 0);
    }

    void test_write_buffer_is_bounded_and_preserves_partial_order()
    {
        WriteBuffer buffer(25, 40);
        const Frame frame = makeFrame(4);
        const auto encoded_size = snf::protocol::encode_frame(frame).size();

        assert(buffer.append(frame) == SendResult::Accepted);
        assert(buffer.queuedByteCount() == encoded_size);
        assert(buffer.frontBytes().size() == encoded_size);

        assert(buffer.append(frame) == SendResult::SoftLimit);
        assert(buffer.queuedByteCount() == encoded_size);

        assert(!buffer.consume(3));
        assert(buffer.queuedByteCount() == encoded_size - 3);
        assert(buffer.frontBytes().size() == encoded_size - 3);
        assert(buffer.consume(encoded_size - 3));
        assert(buffer.empty());

        bool threw = false;
        try
        {
            static_cast<void>(buffer.consume(1));
        }
        catch (const std::out_of_range&)
        {
            threw = true;
        }
        assert(threw);

        WriteBuffer hard_limited(10, 15);
        assert(hard_limited.append(makeFrame(6)) == SendResult::HardLimit);
        assert(hard_limited.empty());

        WriteBuffer critical(10, 20);
        assert(critical.append(makeFrame(2), true) == SendResult::Accepted);
        assert(critical.queuedByteCount() > critical.softWatermark());

        Frame invalid{
            .type = static_cast<MessageType>(0xFFFF),
            .request_id = 1,
            .payload = {},
        };
        assert(critical.append(invalid) == SendResult::Rejected);
    }

    void test_connection_table_raii_and_full_generation_validation()
    {
        ConnectionTable table(ConnectionTableConfig{.capacity = 1});
        auto first_sockets = makeSocketPair();
        auto first_reservation = table.tryReserve(std::move(first_sockets.left), WorkerId{0});
        assert(first_reservation.has_value());
        const ConnectionHandle first_handle = first_reservation->handle();
        assert(table.reservedCount() == 1);
        assert(table.activeCount() == 0);
        assert(table.find(first_handle) == nullptr);

        first_reservation->commit();
        assert(table.reservedCount() == 0);
        assert(table.activeCount() == 1);
        assert(table.find(first_handle) != nullptr);
        assert(!table.hasCapacity());
        assert(table.release(first_handle));
        assert(table.activeCount() == 0);
        assert(table.find(first_handle) == nullptr);

        auto second_sockets = makeSocketPair();
        auto second_reservation = table.tryReserve(std::move(second_sockets.left), WorkerId{0});
        assert(second_reservation.has_value());
        const ConnectionHandle second_handle = second_reservation->handle();
        assert(second_handle.id == first_handle.id);
        assert(second_handle.generation.value > first_handle.generation.value);
        second_reservation->commit();

        // A stale queue item with the old generation cannot address the new slot.
        assert(table.find(first_handle) == nullptr);
        assert(table.find(second_handle) != nullptr);
        assert(table.release(second_handle));

        auto rollback_sockets = makeSocketPair();
        {
            auto rollback_reservation = table.tryReserve(std::move(rollback_sockets.left), WorkerId{0});
            assert(rollback_reservation.has_value());
            assert(table.reservedCount() == 1);
        }
        assert(table.reservedCount() == 0);
        assert(table.activeCount() == 0);
    }

    void test_poll_registration_transaction_and_stale_token_rejection()
    {
        PollRegistrationTable table(2);
        const ConnectionHandle connection{
            .id = ConnectionId{3},
            .generation = ConnectionGeneration{9},
        };

        auto listener = table.tryReserve(10, PollTargetKind::Listener);
        assert(listener.has_value());
        const PollToken stale_token = listener->token();
        const PollRegistrationHandle listener_handle = listener->handle();
        assert(!table.lookup(stale_token).has_value());
        listener->commit();
        assert(table.lookup(stale_token).has_value());
        assert(table.activeCount() == 1);

        auto client = table.tryReserve(11, PollTargetKind::ClientConnection, connection);
        assert(client.has_value());
        const PollToken client_token = client->token();
        assert(!table.lookup(client_token).has_value());
        client->commit();
        assert(table.lookup(client_token)->connection == connection);
        assert(!table.hasCapacity());

        assert(table.release(listener_handle));
        assert(!table.lookup(stale_token).has_value());
        assert(table.release(client->handle()));
        assert(table.activeCount() == 0);

        bool invalid_kind_rejected = false;
        try
        {
            static_cast<void>(table.tryReserve(12, PollTargetKind::Wakeup));
        }
        catch (const std::invalid_argument&)
        {
            invalid_kind_rejected = true;
        }
        assert(invalid_kind_rejected);

        auto rollback = table.tryReserve(13, PollTargetKind::Listener);
        assert(rollback.has_value());
        assert(table.reservedCount() == 1);
        rollback->rollback();
        assert(table.reservedCount() == 0);
    }

    void test_connection_work_queue_is_bounded_fifo_of_handles()
    {
        ConnectionWorkQueue queue(2);
        const ConnectionHandle first{ConnectionId{1}, ConnectionGeneration{10}};
        const ConnectionHandle second{ConnectionId{2}, ConnectionGeneration{20}};
        const ConnectionHandle third{ConnectionId{3}, ConnectionGeneration{30}};

        assert(queue.tryPush(first));
        assert(queue.tryPush(second));
        assert(!queue.tryPush(third));
        assert(queue.size() == 2);
        assert(queue.tryPop().value() == first);
        assert(queue.tryPush(third));
        assert(queue.tryPop().value() == second);
        assert(queue.tryPop().value() == third);
        assert(!queue.tryPop().has_value());
    }

    void test_remote_send_payload_survives_full_inbox_admission_failure()
    {
        InboxLane lane(100);
        const ConnectionRef reference{ConnectionId{7}, ConnectionGeneration{8}, WorkerId{2}};
        const Frame first_frame = makeFrame(5);
        const Frame second_frame = makeFrame(6, MessageType::Pong);

        WorkerEnvelope first{
            .event = RemoteConnectionSend{.connection = reference, .frame = first_frame},
            .charged_bytes = 90,
        };
        assert(lane.tryPush(std::move(first)) == InboxPushResult::Accepted);

        WorkerEnvelope rejected{
            .event = RemoteConnectionSend{.connection = reference, .frame = second_frame, .critical = true},
            .charged_bytes = 20,
        };
        assert(lane.tryPush(std::move(rejected)) == InboxPushResult::Full);
        assert(std::get<RemoteConnectionSend>(rejected.event).frame == second_frame);
        assert(std::get<RemoteConnectionSend>(rejected.event).critical);

        WorkerEnvelope out;
        assert(lane.tryPop(out));
        assert(std::get<RemoteConnectionSend>(out.event).frame == first_frame);
        assert(lane.approximateQueuedBytes() == 0);
    }
}

void run_worker_connection_tests()
{
    test_wire_size_contract();
    test_frame_decoder_reports_logical_buffer_size_and_resets();
    test_write_buffer_is_bounded_and_preserves_partial_order();
    test_connection_table_raii_and_full_generation_validation();
    test_poll_registration_transaction_and_stale_token_rejection();
    test_connection_work_queue_is_bounded_fifo_of_handles();
    test_remote_send_payload_survives_full_inbox_admission_failure();
}
