#include "file_descriptor_test_support.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <cassert>
#include <unistd.h>
#include <utility>

namespace
{

void test_closes_descriptor_when_destroyed()
{
    int pipe_fds[2]{};
    assert(::pipe(pipe_fds) == 0);

    const int read_fd = pipe_fds[0];
    {
        const snf::net::UniqueFileDescriptor owner{read_fd};
        assert(owner.isValid());
        assert(owner.getDescriptor() == read_fd);
    }

    assert(snf::test::is_closed(read_fd));
    assert(::close(pipe_fds[1]) == 0);
}

void test_move_constructor_transfers_ownership()
{
    int pipe_fds[2]{};
    assert(::pipe(pipe_fds) == 0);

    const int read_fd = pipe_fds[0];
    snf::net::UniqueFileDescriptor original{read_fd};
    snf::net::UniqueFileDescriptor moved{std::move(original)};

    assert(!original.isValid());
    assert(moved.isValid());
    assert(moved.getDescriptor() == read_fd);

    moved.init();
    assert(snf::test::is_closed(read_fd));
    assert(::close(pipe_fds[1]) == 0);
}

void test_move_assignment_releases_previous_descriptor()
{
    int first_pipe_fds[2]{};
    int second_pipe_fds[2]{};
    assert(::pipe(first_pipe_fds) == 0);
    assert(::pipe(second_pipe_fds) == 0);

    const int first_read_fd = first_pipe_fds[0];
    const int second_read_fd = second_pipe_fds[0];

    snf::net::UniqueFileDescriptor target{first_read_fd};
    snf::net::UniqueFileDescriptor source{second_read_fd};
    target = std::move(source);

    assert(snf::test::is_closed(first_read_fd));
    assert(!source.isValid());
    assert(target.getDescriptor() == second_read_fd);

    target.init();
    assert(snf::test::is_closed(second_read_fd));
    assert(::close(first_pipe_fds[1]) == 0);
    assert(::close(second_pipe_fds[1]) == 0);
}

void test_release_transfers_closing_responsibility()
{
    int pipe_fds[2]{};
    assert(::pipe(pipe_fds) == 0);

    snf::net::UniqueFileDescriptor owner{pipe_fds[0]};
    const int released_fd = owner.release();

    assert(!owner.isValid());
    assert(::fcntl(released_fd, F_GETFD) != -1);
    assert(::close(released_fd) == 0);
    assert(::close(pipe_fds[1]) == 0);
}

} // namespace

void run_unique_file_descriptor_tests()
{
    test_closes_descriptor_when_destroyed();
    test_move_constructor_transfers_ownership();
    test_move_assignment_releases_previous_descriptor();
    test_release_transfers_closing_responsibility();
}
