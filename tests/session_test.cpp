#include "file_descriptor_test_support.hpp"
#include "snf/net/session.hpp"
#include "snf/protocol/frame_codec.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <span>
#include <unistd.h>
#include <utility>

namespace
{
    void test_session_keeps_partially_received_frame()
    {
        int pipe_file_descriptors[2]{};
        assert(::pipe(pipe_file_descriptors) == 0);

        snf::net::Session session{
            snf::net::UniqueFileDescriptor{pipe_file_descriptors[0]},
        };

        const snf::protocol::Frame frame{.type = snf::protocol::MessageType::Ping, .request_id = 1, .payload = {std::byte{0xAA}, std::byte{0xBB}}};

        const auto encoded = snf::protocol::encode_frame(frame);
        const std::span<const std::byte> encoded_view{encoded};
        constexpr std::size_t first_chunk_size = 5;

        const auto first_result = session.appendReceivedBytes(encoded_view.first(first_chunk_size));

        assert(first_result.ok());
        assert(first_result.frames.empty());

        const auto second_result = session.appendReceivedBytes(encoded_view.subspan(first_chunk_size));

        assert(second_result.ok());
        assert(second_result.frames.size() == 1);
        assert(second_result.frames[0].type == frame.type);
        assert(second_result.frames[0].request_id == frame.request_id);
        assert(second_result.frames[0].payload == frame.payload);

        assert(::close(pipe_file_descriptors[1]) == 0);
    }

    void test_session_takes_socket_ownership()
    {
        int pipe_file_descriptors[2]{};
        assert(::pipe(pipe_file_descriptors) == 0);

        const int read_file_descriptor = pipe_file_descriptors[0];
        snf::net::UniqueFileDescriptor socket{read_file_descriptor};
        const snf::net::ConnectionId connection_id{
            .descriptor = read_file_descriptor,
            .generation = 99,
        };

        {
            snf::net::Session session{std::move(socket), connection_id};

            assert(!socket.isValid());
            assert(session.getDescriptor() == read_file_descriptor);
            assert(session.getConnectionId() == connection_id);
        }

        assert(snf::test::is_closed(read_file_descriptor));
        assert(::close(pipe_file_descriptors[1]) == 0);
    }

    void test_session_keeps_partial_send_offset()
    {
        int pipe_file_descriptors[2]{};
        assert(::pipe(pipe_file_descriptors) == 0);

        snf::net::Session session{
            snf::net::UniqueFileDescriptor{pipe_file_descriptors[0]},
        };

        const snf::protocol::Frame frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 7,
            .payload = {std::byte{0xAA}, std::byte{0xBB}},
        };

        const auto encoded_frame = snf::protocol::encode_frame(frame);
        assert(session.enqueueFrame(frame));
        assert(session.getPendingSendByteCount() == encoded_frame.size());
        assert(std::ranges::equal(session.getPendingSendBytes(), encoded_frame));

        constexpr std::size_t sent_byte_count = 5;
        assert(!session.consumeSentBytes(sent_byte_count));

        assert(session.getPendingSendByteCount() == encoded_frame.size() - sent_byte_count);
        assert(std::ranges::equal(session.getPendingSendBytes(), std::span<const std::byte>{encoded_frame}.subspan(sent_byte_count)));

        assert(session.consumeSentBytes(encoded_frame.size() - sent_byte_count));
        assert(!session.hasPendingSend());
        assert(session.getPendingSendByteCount() == 0);

        assert(::close(pipe_file_descriptors[1]) == 0);
    }

    void test_session_preserves_send_order()
    {
        int pipe_file_descriptors[2]{};
        assert(::pipe(pipe_file_descriptors) == 0);

        snf::net::Session session{
            snf::net::UniqueFileDescriptor{pipe_file_descriptors[0]},
        };

        const snf::protocol::Frame first_frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        };
        const snf::protocol::Frame second_frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 2,
            .payload = {},
        };

        const auto first_encoded_frame = snf::protocol::encode_frame(first_frame);
        const auto second_encoded_frame = snf::protocol::encode_frame(second_frame);

        assert(session.enqueueFrame(first_frame));
        assert(session.enqueueFrame(second_frame));
        assert(std::ranges::equal(session.getPendingSendBytes(), first_encoded_frame));

        assert(session.consumeSentBytes(first_encoded_frame.size()));
        assert(std::ranges::equal(session.getPendingSendBytes(), second_encoded_frame));

        assert(::close(pipe_file_descriptors[1]) == 0);
    }

    void test_session_rejects_send_queue_over_limit()
    {
        int pipe_file_descriptors[2]{};
        assert(::pipe(pipe_file_descriptors) == 0);

        constexpr std::size_t max_pending_send_bytes = 10;
        snf::net::Session session{
            snf::net::UniqueFileDescriptor{pipe_file_descriptors[0]},
            max_pending_send_bytes,
        };

        const snf::protocol::Frame frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {std::byte{0xAA}},
        };

        assert(!session.enqueueFrame(frame));
        assert(!session.hasPendingSend());
        assert(session.getPendingSendByteCount() == 0);

        assert(::close(pipe_file_descriptors[1]) == 0);
    }
} // namespace

void run_session_tests()
{
    test_session_takes_socket_ownership();
    test_session_keeps_partially_received_frame();
    test_session_keeps_partial_send_offset();
    test_session_preserves_send_order();
    test_session_rejects_send_queue_over_limit();
}
