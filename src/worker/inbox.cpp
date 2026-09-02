#include "snf/worker/inbox.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <new>
#include <stdexcept>

namespace snf::worker
{
    InboxLane::InboxLane(const std::uint64_t max_queued_bytes) noexcept
        : _max_queued_bytes(max_queued_bytes)
    {
    }

    InboxLane::~InboxLane()
    {
        std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
        const std::uint64_t tail = _producer.tail.load(std::memory_order_relaxed);
        while (head != tail)
        {
            const std::size_t index = static_cast<std::size_t>(head & (CAPACITY - 1));
            std::destroy_at(reinterpret_cast<WorkerEnvelope*>(_slots[index].storage));
            ++head;
        }
    }

    void InboxLane::configure(const std::uint64_t max_queued_bytes) noexcept
    {
        _max_queued_bytes = max_queued_bytes;
    }

    InboxPushResult InboxLane::tryPush(WorkerEnvelope&& envelope) noexcept
    {
        if (_closed.load(std::memory_order_acquire))
        {
            return InboxPushResult::Closed;
        }

        const std::uint64_t tail = _producer.tail.load(std::memory_order_relaxed);
        const std::uint64_t head = _consumer.head.load(std::memory_order_acquire);
        if (tail - head >= CAPACITY)
        {
            return InboxPushResult::Full;
        }

        const std::uint64_t produced = _producer.produced_bytes.load(std::memory_order_relaxed);
        const std::uint64_t consumed = _consumer.consumed_bytes.load(std::memory_order_acquire);
        const std::uint64_t queued_bytes = produced >= consumed ? produced - consumed : 0;
        if (queued_bytes + envelope.charged_bytes > _max_queued_bytes)
        {
            return InboxPushResult::Full;
        }

        const std::size_t index = static_cast<std::size_t>(tail & (CAPACITY - 1));
        const std::uint32_t charged = envelope.charged_bytes;
        std::construct_at(reinterpret_cast<WorkerEnvelope*>(_slots[index].storage), std::move(envelope));

        _producer.produced_bytes.store(produced + charged, std::memory_order_relaxed);
        _producer.tail.store(tail + 1, std::memory_order_release);
        return InboxPushResult::Accepted;
    }

    bool InboxLane::tryPop(WorkerEnvelope& out) noexcept
    {
        const std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
        const std::uint64_t tail = _producer.tail.load(std::memory_order_acquire);
        if (head == tail)
        {
            return false;
        }

        const std::size_t index = static_cast<std::size_t>(head & (CAPACITY - 1));
        auto* slot_ptr = reinterpret_cast<WorkerEnvelope*>(_slots[index].storage);
        const std::uint32_t charged = slot_ptr->charged_bytes;
        out = std::move(*slot_ptr);
        std::destroy_at(slot_ptr);

        const std::uint64_t consumed = _consumer.consumed_bytes.load(std::memory_order_relaxed);
        _consumer.consumed_bytes.store(consumed + charged, std::memory_order_relaxed);
        _consumer.head.store(head + 1, std::memory_order_release);
        return true;
    }

    void InboxLane::close() noexcept
    {
        _closed.store(true, std::memory_order_release);
    }

    std::uint64_t InboxLane::approximateQueuedBytes() const noexcept
    {
        const std::uint64_t produced = _producer.produced_bytes.load(std::memory_order_relaxed);
        const std::uint64_t consumed = _consumer.consumed_bytes.load(std::memory_order_relaxed);
        return produced >= consumed ? produced - consumed : 0;
    }

    std::uint64_t InboxLane::maxQueuedBytes() const noexcept
    {
        return _max_queued_bytes;
    }

    bool InboxLane::isEmpty() const noexcept
    {
        const std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
        const std::uint64_t tail = _producer.tail.load(std::memory_order_acquire);
        return head == tail;
    }

    bool WorkerInboxPort::isBound() const noexcept
    {
        return _lane != nullptr;
    }

    InboxPushResult WorkerInboxPort::tryPush(WorkerEnvelope&& envelope) noexcept
    {
        // 바인딩되지 않은 포트를 쓰는 것은 startup 배선 오류다. release 빌드에서는
        // Closed 로 떨어뜨려 event 를 조용히 잃지 않게 한다.
        assert(_lane != nullptr);
        if (_lane == nullptr)
        {
            return InboxPushResult::Closed;
        }

        const auto result = _lane->tryPush(std::move(envelope));
        if (result == InboxPushResult::Accepted && _wakeup != nullptr)
        {
            _wakeup->notify();
        }
        return result;
    }

    WorkerInbox::WorkerInbox(const std::uint16_t worker_count, WakeupHandle& wakeup, const WorkerInboxConfig& config)
        : _worker_count(worker_count)
        , _wakeup(&wakeup)
    {
        if (worker_count == 0 || worker_count > config.max_workers || config.max_bytes_per_worker == 0)
        {
            throw std::invalid_argument{"Invalid WorkerInbox configuration"};
        }

        // lane 은 source id 로 직접 인덱싱하므로 자기 자신용 lane 도 함께 할당한다.
        // 자기 lane 은 사용하지 않지만(INV-03: local tell 은 mailbox 를 통과한다) 인덱싱이 단순해진다.
        //
        // ring slot memory   = worker_count * worker_count * CAPACITY * sizeof(WorkerEnvelope)
        // 최대 queued payload = worker_count * max_bytes_per_worker
        // 두 값의 합이 프로세스 메모리 예산 안에 들어가야 한다.
        const std::uint64_t max_bytes_per_lane = config.max_bytes_per_worker / worker_count;

        _lanes = std::make_unique<InboxLane[]>(worker_count);
        for (std::uint16_t i = 0; i < worker_count; ++i)
        {
            _lanes[i].configure(max_bytes_per_lane);
        }

        _bound = std::make_unique<bool[]>(worker_count);
        for (std::uint16_t i = 0; i < worker_count; ++i)
        {
            _bound[i] = false;
        }
    }

    WorkerInboxPort WorkerInbox::bindSource(const WorkerId source) noexcept
    {
        assert(source.value < _worker_count);
        if (source.value >= _worker_count)
        {
            return {};
        }
        assert(!_bound[source.value]);
        if (_bound[source.value])
        {
            return {};
        }
        _bound[source.value] = true;

        WorkerInboxPort port;
        port._lane = &_lanes[source.value];
        port._wakeup = _wakeup;
        return port;
    }

    void WorkerInbox::close() noexcept
    {
        for (std::uint16_t i = 0; i < _worker_count; ++i)
        {
            _lanes[i].close();
        }
    }

    bool WorkerInbox::isEmpty() const noexcept
    {
        for (std::uint16_t i = 0; i < _worker_count; ++i)
        {
            if (!_lanes[i].isEmpty())
            {
                return false;
            }
        }
        return true;
    }

    std::uint64_t WorkerInbox::approximateQueuedBytes() const noexcept
    {
        std::uint64_t total = 0;
        for (std::uint16_t i = 0; i < _worker_count; ++i)
        {
            total += _lanes[i].approximateQueuedBytes();
        }
        return total;
    }

    std::uint64_t WorkerInbox::maxQueuedBytesTotal() const noexcept
    {
        std::uint64_t total = 0;
        for (std::uint16_t i = 0; i < _worker_count; ++i)
        {
            total += _lanes[i].maxQueuedBytes();
        }
        return total;
    }

    std::uint16_t WorkerInbox::workerCount() const noexcept
    {
        return _worker_count;
    }

    InboxLane& WorkerInbox::lane(const std::size_t index) noexcept
    {
        assert(index < _worker_count);
        return _lanes[index];
    }

    const InboxLane& WorkerInbox::lane(const std::size_t index) const noexcept
    {
        assert(index < _worker_count);
        return _lanes[index];
    }
}
