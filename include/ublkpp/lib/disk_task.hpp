#pragma once

#include <coroutine>
#include <utility>

namespace ublkpp {

// Lightweight lazy coroutine task for per-disk async I/O. Composable via symmetric transfer:
// co_await disk_task<T> in a co_io_job or another disk_task<U> suspends the caller and
// immediately resumes the callee without going through any scheduler.
//
// run_queue_loop drives all resumption: CQEs install a handle in CqeState::waiter and call
// h.resume(), which propagates back to the caller via final_suspend symmetric transfer.
//
// Lifetime: the coroutine frame is owned by the disk_task object. Move-only; the caller
// must co_await (consuming the task) before the object is destroyed.
template < typename T >
struct disk_task {
    struct promise_type {
        T _value{};
        std::coroutine_handle<> _continuation{};

        disk_task get_return_object() noexcept {
            return disk_task{std::coroutine_handle< promise_type >::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }

        struct final_awaiter {
            bool await_ready() noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle< promise_type > h) noexcept {
                auto cont = h.promise()._continuation;
                return cont ? cont : std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }

        void return_value(T v) noexcept { _value = v; }
        [[noreturn]] void unhandled_exception() { throw; }
    };

    std::coroutine_handle< promise_type > _coro;

    explicit disk_task(std::coroutine_handle< promise_type > h) noexcept : _coro(h) {}
    disk_task(disk_task&& o) noexcept : _coro(std::exchange(o._coro, {})) {}
    disk_task(disk_task const&) = delete;
    ~disk_task() {
        if (_coro) _coro.destroy();
    }

    // Awaitable interface: allows co_await disk_task<T> from co_io_job or disk_task<U>.
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
        _coro.promise()._continuation = cont;
        return _coro; // symmetric transfer: start callee immediately
    }
    T await_resume() noexcept { return _coro.promise()._value; }
};

} // namespace ublkpp
