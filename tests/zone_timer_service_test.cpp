#include "snf/server/zone_timer_service.hpp"

#include <cassert>
#include <chrono>
#include <deque>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    class ManualClock final : public snf::server::TimerClock
    {
    public:
        [[nodiscard]] snf::server::TimerTimePoint now() const noexcept override
        {
            return current;
        }

        void advance(const std::chrono::steady_clock::duration duration)
        {
            current += duration;
        }

        snf::server::TimerTimePoint current{};
    };

    struct RecordedTimerCommand
    {
        snf::server::ZoneId zone;
        snf::server::ZoneCommand command;
    };

    class RecordingTimerSink final : public snf::server::ZoneTimerSink
    {
    public:
        [[nodiscard]] snf::runtime::PostResult
        tryPostTimerCommand(const snf::server::ZoneId zone,
                            snf::server::ZoneCommand command) override
        {
            commands.push_back(RecordedTimerCommand{.zone = zone, .command = std::move(command)});
            if (throw_on_call)
            {
                throw std::runtime_error{"timer sink failure"};
            }
            if (results.empty())
            {
                return fallback;
            }

            const snf::runtime::PostResult result = results.front();
            results.pop_front();
            return result;
        }

        std::vector<RecordedTimerCommand> commands;
        std::deque<snf::runtime::PostResult> results;
        snf::runtime::PostResult fallback{snf::runtime::PostResult::Accepted};
        bool throw_on_call{false};
    };

    void test_periodic_timer_uses_injected_time_without_catch_up()
    {
        ManualClock clock;
        RecordingTimerSink sink;
        snf::server::ZoneTimerService timers{
            sink,
            clock,
            snf::server::ZoneTimerServiceConfig{
                .tick_interval = 10ms,
                .cancellation_retry_interval = 1ms,
                .max_timers = 2,
                .on_failure = {},
            },
        };
        const snf::server::ZoneId zone{.value = 4};

        const auto registration = timers.tryEnsureTimer(zone);
        assert(registration.result == snf::runtime::PostResult::Accepted);
        assert(registration.timer.has_value());
        assert(registration.created);
        assert(sink.commands.size() == 1);
        const auto* arm =
            std::get_if<snf::server::ArmZoneSimulationTimer>(&sink.commands.back().command);
        assert(arm != nullptr);
        assert(arm->timer == registration.timer);

        const auto repeated = timers.tryEnsureTimer(zone);
        assert(repeated.result == snf::runtime::PostResult::Accepted);
        assert(repeated.timer == registration.timer);
        assert(!repeated.created);
        assert(sink.commands.size() == 1);

        timers.dispatchDue();
        assert(sink.commands.size() == 1);
        clock.advance(10ms);
        timers.dispatchDue();
        assert(sink.commands.size() == 2);
        const auto* first_tick =
            std::get_if<snf::server::ZoneSimulationTick>(&sink.commands.back().command);
        assert(first_tick != nullptr);
        assert(first_tick->timer == registration.timer);
        assert(first_tick->tick == 1);

        clock.advance(35ms);
        timers.dispatchDue();
        const auto* second_tick =
            std::get_if<snf::server::ZoneSimulationTick>(&sink.commands.back().command);
        assert(second_tick != nullptr);
        assert(second_tick->tick == 2);
        assert(timers.stats().fired == 2);
        assert(timers.stats().skipped_intervals == 2);
    }

    void test_timer_capacity_and_cancel_retry_are_bounded()
    {
        ManualClock clock;
        RecordingTimerSink sink;
        snf::server::ZoneTimerService timers{
            sink,
            clock,
            snf::server::ZoneTimerServiceConfig{
                .tick_interval = 10ms,
                .cancellation_retry_interval = 1ms,
                .max_timers = 1,
                .on_failure = {},
            },
        };
        const snf::server::ZoneId first{.value = 1};
        const snf::server::ZoneId second{.value = 2};
        assert(timers.tryEnsureTimer(first).result == snf::runtime::PostResult::Accepted);
        assert(timers.tryEnsureTimer(second).result == snf::runtime::PostResult::Full);

        sink.results.push_back(snf::runtime::PostResult::Full);
        assert(timers.tryCancelTimer(first) == snf::runtime::PostResult::Full);
        assert(timers.stats().pending_cancellations == 1);
        timers.dispatchDue();
        assert(timers.stats().pending_cancellations == 0);
        assert(timers.stats().cancelled == 1);
        assert(timers.stats().cancellation_retries == 1);

        const auto replacement = timers.tryEnsureTimer(second);
        assert(replacement.result == snf::runtime::PostResult::Accepted);
        assert(replacement.timer.has_value());
        assert(replacement.timer->value == 2);
    }

    void test_zero_interval_disables_timers_without_faking_an_identity()
    {
        ManualClock clock;
        RecordingTimerSink sink;
        snf::server::ZoneTimerService timers{
            sink,
            clock,
            snf::server::ZoneTimerServiceConfig{
                .tick_interval = 0ms,
                .cancellation_retry_interval = 1ms,
                .max_timers = 1,
                .on_failure = {},
            },
        };

        const auto registration = timers.tryEnsureTimer(snf::server::ZoneId{.value = 1});
        assert(registration.result == snf::runtime::PostResult::Accepted);
        assert(!registration.timer.has_value());
        assert(!registration.created);
        assert(sink.commands.empty());
        assert(timers.stats().active_timers == 0);
    }

    void test_timer_delivery_failure_is_reported_once_and_rethrown()
    {
        ManualClock clock;
        RecordingTimerSink sink;
        int failure_notifications = 0;
        snf::server::ZoneTimerService timers{
            sink,
            clock,
            snf::server::ZoneTimerServiceConfig{
                .tick_interval = 10ms,
                .cancellation_retry_interval = 1ms,
                .max_timers = 1,
                .on_failure = [&failure_notifications] { ++failure_notifications; },
            },
        };
        assert(timers.tryEnsureTimer(snf::server::ZoneId{.value = 1}).result ==
               snf::runtime::PostResult::Accepted);

        sink.throw_on_call = true;
        clock.advance(10ms);
        timers.dispatchDue();
        timers.dispatchDue();
        assert(timers.stats().failures == 1);
        assert(failure_notifications == 1);

        bool rethrown = false;
        try
        {
            timers.rethrowIfFailed();
        }
        catch (const std::runtime_error& error)
        {
            rethrown = std::string_view{error.what()} == "timer sink failure";
        }
        assert(rethrown);
    }
}

void run_zone_timer_service_tests()
{
    test_periodic_timer_uses_injected_time_without_catch_up();
    test_timer_capacity_and_cancel_retry_are_bounded();
    test_zero_interval_disables_timers_without_faking_an_identity();
    test_timer_delivery_failure_is_reported_once_and_rethrown();
}
