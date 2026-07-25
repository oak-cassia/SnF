#include "file_descriptor_test_support.hpp"
#include "snf/net/session.hpp"
#include "snf/protocol/frame_codec.hpp"

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

        const snf::protocol::Frame frame{.type = snf::protocol::MessageType::Ping,
                                         .request_id = 1,
                                         .payload = {std::byte{0xAA}, std::byte{0xBB}}};

        const auto encoded = snf::protocol::encode_frame(frame);
        const std::span<const std::byte> encoded_view{encoded};
        constexpr std::size_t first_chunk_size = 5;

        const auto first_result = session.appendReceivedBytes(encoded_view.first(first_chunk_size));

        assert(first_result.ok());
        assert(first_result.frames.empty());

        const auto second_result =
            session.appendReceivedBytes(encoded_view.subspan(first_chunk_size));

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

        {
            snf::net::Session session{std::move(socket)};

            assert(!socket.isValid());
            assert(session.getDescriptor() == read_file_descriptor);
        }

        assert(snf::test::is_closed(read_file_descriptor));
        assert(::close(pipe_file_descriptors[1]) == 0);
    }
} // namespace

void run_session_tests()
{
    test_session_takes_socket_ownership();
    test_session_keeps_partially_received_frame();
}
