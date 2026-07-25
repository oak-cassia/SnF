#pragma once

namespace snf::net
{
    class UniqueFileDescriptor
    {
    public:
       static constexpr int INVALID_FD = -1;

        explicit UniqueFileDescriptor(int file_descriptor = INVALID_FD) noexcept;
        ~UniqueFileDescriptor();

        UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
        UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;

        UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept;
        UniqueFileDescriptor& operator=(UniqueFileDescriptor&& other) noexcept;

        [[nodiscard]] int getDescriptor() const noexcept;
        [[nodiscard]] bool isValid() const noexcept;

        [[nodiscard]] int release() noexcept;
        void init(int fd = INVALID_FD) noexcept;

    private:
        int _fd;
    };
}
