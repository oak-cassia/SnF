#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/server/outbound_channel.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#if defined(__linux__)
#include <sys/eventfd.h>
#else
#include <unistd.h>
#endif
#include <vector>

namespace snf::test
{
    inline snf::net::UniqueFileDescriptor make_wake_descriptor()
    {
#if defined(__linux__)
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
#else
        int fds[2];
        const int res = ::pipe(fds);
        assert(res != -1);
        return snf::net::UniqueFileDescriptor{fds[1]};
#endif
    }

    // Stands in for the runtime's continuation queue. A channel only ever reaches an
    // endpoint through a producer, so recording publishes and rejections is enough to
    // tell a granted waiter from a discarded award.
    class RecordingContinuationEndpoint final : public snf::runtime::ContinuationEndpoint
    {
    public:
        [[nodiscard]] bool publish(const snf::runtime::ActorContinuation& continuation) noexcept override
        {
            published.push_back(continuation);
            return true;
        }

        void reportRejectedCompletion(const snf::runtime::ActorContinuation&, const snf::runtime::ContinuationRejection rejection) noexcept override
        {
            ++rejected_count;
            last_rejection = rejection;
        }

        std::vector<snf::runtime::ActorContinuation> published;
        std::size_t rejected_count{0};
        std::optional<snf::runtime::ContinuationRejection> last_rejection;
    };

    // One suspended actor's half of a reservation await: the state its owning Worker
    // would read, plus the producer the channel keeps.
    struct ReservationWaiter
    {
        using State = snf::runtime::AsyncOperationState<snf::server::OutboundReservation>;
        using Producer = snf::runtime::AsyncOperationProducer<snf::server::OutboundReservation>;

        ReservationWaiter(const std::shared_ptr<RecordingContinuationEndpoint>& endpoint, const std::uint64_t task_id)
            : state(std::make_shared<State>())
            , producer(state,
                       snf::runtime::ActorCompletionHandle{
                           .endpoint = endpoint,
                           .continuation =
                               snf::runtime::ActorContinuation{
                                   .target =
                                       snf::runtime::ActorKey{
                                           .kind = snf::runtime::ActorKind::ProvisionalPlayer,
                                           .entity = task_id,
                                       },
                                   .incarnation = snf::runtime::ActorIncarnation{.value = 1},
                                   .task = snf::runtime::TaskId{.value = task_id},
                               },
                       })
        {
        }

        [[nodiscard]] bool isPending() const
        {
            return state->outcome() == snf::runtime::AsyncOperationOutcome::Pending;
        }

        [[nodiscard]] bool isCompleted() const
        {
            return state->outcome() == snf::runtime::AsyncOperationOutcome::Completed;
        }

        std::shared_ptr<State> state;
        Producer producer;
    };
}
