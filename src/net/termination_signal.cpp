#include "snf/net/termination_signal.hpp"

#include "snf/net/system_error.hpp"

#include <csignal>
#include <sys/signalfd.h>

namespace snf::net
{
    UniqueFileDescriptor create_termination_signal_listener()
    {
        sigset_t termination_signals{};
        ::sigemptyset(&termination_signals);
        ::sigaddset(&termination_signals, SIGINT);
        ::sigaddset(&termination_signals, SIGTERM);

        if (::sigprocmask(SIG_BLOCK, &termination_signals, nullptr) == -1)
        {
            throw_system_error("sigprocmask(SIG_BLOCK)");
        }

        const int signal_descriptor =
            ::signalfd(-1, &termination_signals, SFD_NONBLOCK | SFD_CLOEXEC);
        if (signal_descriptor == -1)
        {
            throw_system_error("signalfd");
        }

        return UniqueFileDescriptor{signal_descriptor};
    }
}
