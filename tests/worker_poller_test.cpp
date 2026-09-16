#include "snf/worker/poll_token.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/wakeup.hpp"

#include <cassert>
#include <chrono>

namespace
{
    using namespace snf::worker;

    static_assert(encodePollToken(PollTargetKind::Wakeup, 0, 0) == 0x0100000000000000ULL);
    static_assert(encodePollToken(PollTargetKind::ClientConnection, 5, 7) == 0x0200000500000007ULL);
    static_assert(encodePollToken(PollTargetKind::ClientConnection, MAX_POLL_INDEX, MAX_POLL_GENERATION) == 0x02ffffffffffffffULL);
    static_assert(encodePollToken(PollTargetKind::DbConnection, 0xABCD, 0xDEADBEEF) == 0x0300abcddeadbeefULL);
    static_assert(encodePollToken(PollTargetKind::DbConnection, 1, 1) == 0x0300000100000001ULL);

    static_assert(decodePollToken(0x0300abcddeadbeefULL) == PollToken{PollTargetKind::DbConnection, 0xABCD, 0xDEADBEEF});
    static_assert(decodePollToken(0ULL).kind == PollTargetKind::Invalid);

    void test_poll_token_roundtrip()
    {
        const PollTargetKind kinds[] = {
            PollTargetKind::Wakeup,
            PollTargetKind::ClientConnection,
            PollTargetKind::DbConnection,
        };

        const std::uint32_t indices[] = {0, 1, 42, 0x12345, MAX_POLL_INDEX};
        const std::uint32_t generations[] = {0, 1, 100, 0xDEADBEEF, MAX_POLL_GENERATION};

        for (const auto kind : kinds)
        {
            for (const auto index : indices)
            {
                for (const auto generation : generations)
                {
                    const auto raw = encodePollToken(kind, index, generation);
                    const auto decoded = decodePollToken(raw);
                    assert(decoded.kind == kind);
                    assert(decoded.index == index);
                    assert(decoded.generation == generation);
                }
            }
        }
    }

    void test_wakeup_notify_makes_poller_readable()
    {
        WakeupHandle wakeup;
        Poller poller(16);
        const PollToken token{PollTargetKind::Wakeup, 0, 0};

        poller.add(wakeup.descriptor(), token, PollInterest{.read = true, .write = false});
        wakeup.notify();

        const auto events = poller.wait(std::chrono::milliseconds(0));
        assert(events.size() == 1);
        assert(events[0].token == token);
        assert(events[0].readable);
        assert(!events[0].writable);
        assert(!events[0].error);

        wakeup.consume();
    }

    void test_wakeup_consume_clears_readiness()
    {
        WakeupHandle wakeup;
        Poller poller(16);
        const PollToken token{PollTargetKind::Wakeup, 0, 0};

        poller.add(wakeup.descriptor(), token, PollInterest{.read = true, .write = false});
        wakeup.notify();

        const auto events1 = poller.wait(std::chrono::milliseconds(0));
        assert(events1.size() == 1);

        wakeup.consume();

        const auto events2 = poller.wait(std::chrono::milliseconds(0));
        assert(events2.empty());
    }

    void test_wakeup_notify_is_idempotent_before_consume()
    {
        WakeupHandle wakeup;
        Poller poller(16);
        const PollToken token{PollTargetKind::Wakeup, 0, 0};

        poller.add(wakeup.descriptor(), token, PollInterest{.read = true, .write = false});
        wakeup.notify();
        wakeup.notify();
        wakeup.notify();

        const auto events1 = poller.wait(std::chrono::milliseconds(0));
        assert(events1.size() == 1);

        wakeup.consume();

        const auto events2 = poller.wait(std::chrono::milliseconds(0));
        assert(events2.empty());
    }

    void test_poller_wait_with_no_interest_returns_empty()
    {
        Poller poller(16);
        const auto events = poller.wait(std::chrono::milliseconds(0));
        assert(events.empty());
    }

    void test_poller_remove_stops_delivery()
    {
        WakeupHandle wakeup;
        Poller poller(16);
        const PollToken token{PollTargetKind::Wakeup, 0, 0};

        poller.add(wakeup.descriptor(), token, PollInterest{.read = true, .write = false});
        poller.remove(wakeup.descriptor());

        wakeup.notify();

        const auto events = poller.wait(std::chrono::milliseconds(0));
        assert(events.empty());

        wakeup.consume();
    }

    void test_poller_modify_changes_interest()
    {
        WakeupHandle wakeup;
        Poller poller(16);
        const PollToken token{PollTargetKind::Wakeup, 0, 0};

        poller.add(wakeup.descriptor(), token, PollInterest{.read = true, .write = false});
        poller.modify(wakeup.descriptor(), token, PollInterest{.read = true, .write = true});

        const auto events = poller.wait(std::chrono::milliseconds(0));
        assert(events.size() == 1);
        assert(events[0].token == token);
        assert(events[0].writable);
    }
}

void run_worker_poller_tests()
{
    test_poll_token_roundtrip();
    test_wakeup_notify_makes_poller_readable();
    test_wakeup_consume_clears_readiness();
    test_wakeup_notify_is_idempotent_before_consume();
    test_poller_wait_with_no_interest_returns_empty();
    test_poller_remove_stops_delivery();
    test_poller_modify_changes_interest();
}
