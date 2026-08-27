/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <seastar/core/thread.hh>
#include <seastar/core/do_with.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/shared_mutex.hh>
#include <ranges>
#include <stdexcept>

#include "expected_exception.hh"

using namespace seastar;
using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_semaphore_consume) {
    semaphore sem(0);
    sem.consume(1);
    BOOST_REQUIRE_EQUAL(sem.current(), 0u);
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);

    BOOST_REQUIRE_EQUAL(sem.try_wait(0), false);
    auto fut = sem.wait(1);
    BOOST_REQUIRE_EQUAL(fut.available(), false);
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);
    sem.signal(2);
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_semaphore_1) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        (void)x.first.wait().then([&x] {
            x.second++;
        });
        x.first.signal();
        return sleep(10ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 1);
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_2) {
    auto sem = std::make_optional<semaphore>(0);
    int x = 0;
    auto fut = sem->wait().then([&x] {
        x++;
    });
    sleep(10ms).get();
    BOOST_REQUIRE_EQUAL(x, 0);
    sem = std::nullopt;
    BOOST_CHECK_THROW(fut.get(), broken_promise);
}

SEASTAR_TEST_CASE(test_semaphore_timeout_1) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        (void)x.first.wait(100ms).then([&x] {
            x.second++;
        });
        (void)sleep(3ms).then([&x] {
            x.first.signal();
        });
        return sleep(200ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 1);
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_timeout_2) {
    auto sem = semaphore(0);
    int x = 0;
    auto fut1 = sem.wait(3ms).then([&x] {
        x++;
    });
    bool signaled = false;
    auto fut2 = sleep(100ms).then([&sem, &signaled] {
        signaled = true;
        sem.signal();
    });
    sleep(200ms).get();
    fut2.get();
    BOOST_REQUIRE_EQUAL(signaled, true);
    BOOST_CHECK_THROW(fut1.get(), semaphore_timed_out);
    BOOST_REQUIRE_EQUAL(x, 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_mix_1) {
    auto sem = semaphore(0);
    int x = 0;
    auto fut1 = sem.wait(30ms).then([&x] {
        x++;
    });
    auto fut2 = sem.wait().then([&x] {
        x += 10;
    });
    auto fut3 = sleep(100ms).then([&sem] {
        sem.signal();
    });
    sleep(200ms).get();
    fut3.get();
    fut2.get();
    BOOST_CHECK_THROW(fut1.get(), semaphore_timed_out);
    BOOST_REQUIRE_EQUAL(x, 10);
}

SEASTAR_TEST_CASE(test_broken_semaphore) {
    auto sem = make_lw_shared<semaphore>(0);
    struct oops {};
    auto check_result = [sem] (future<> f) {
        try {
            f.get();
            BOOST_FAIL("expecting exception");
        } catch (oops& x) {
            // ok
            return make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("wrong exception seen");
        }
        BOOST_FAIL("unreachable");
        return make_ready_future<>();
    };
    auto ret = sem->wait().then_wrapped(check_result);
    sem->broken(oops());
    return sem->wait().then_wrapped(check_result).then([ret = std::move(ret)] () mutable {
        return std::move(ret);
    });
}

SEASTAR_THREAD_TEST_CASE(test_default_broken_semaphore) {
    struct test_semaphore_exception_factory {
        static semaphore_timed_out timeout() noexcept { return semaphore_timed_out(); }
    };
    auto sem = basic_semaphore<test_semaphore_exception_factory>(0);
    auto fut = sem.wait();
    BOOST_REQUIRE(!fut.available());
    sem.broken();
    BOOST_REQUIRE_THROW(fut.get(), broken_semaphore);
    BOOST_REQUIRE_THROW(sem.wait().get(), broken_semaphore);
}

SEASTAR_THREAD_TEST_CASE(test_non_default_broken_semaphore) {
    struct test_semaphore_exception_factory {
        static semaphore_timed_out timeout() noexcept { return semaphore_timed_out(); }
    };
    auto sem = basic_semaphore<test_semaphore_exception_factory>(0);
    auto fut = sem.wait();
    BOOST_REQUIRE(!fut.available());
    sem.broken(expected_exception());
    BOOST_REQUIRE_THROW(fut.get(), expected_exception);
    BOOST_REQUIRE_THROW(sem.wait().get(), expected_exception);
}

SEASTAR_TEST_CASE(test_shared_mutex_exclusive) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        return parallel_for_each(std::views::iota(0, 10), [&sm, &counter] (int idx) {
            return with_lock(sm, [&counter] {
                BOOST_REQUIRE_EQUAL(counter, 0u);
                ++counter;
                return sleep(10ms).then([&counter] {
                    --counter;
                    BOOST_REQUIRE_EQUAL(counter, 0u);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_shared_mutex_shared) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        auto running_in_parallel = [&sm, &counter] (int instance) {
            return with_shared(sm, [&counter] {
                ++counter;
                return sleep(10ms).then([&counter] {
                    bool was_parallel = counter != 0;
                    --counter;
                    return was_parallel;
                });
            });
        };
        return map_reduce(std::views::iota(0, 100), running_in_parallel, false, std::bit_or<bool>()).then([&counter] (bool result) {
            BOOST_REQUIRE_EQUAL(result, true);
            BOOST_REQUIRE_EQUAL(counter, 0u);
        });
    });
}

SEASTAR_TEST_CASE(test_shared_mutex_mixed) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        auto running_in_parallel = [&sm, &counter] (int instance) {
            return with_shared(sm, [&counter] {
                ++counter;
                return sleep(10ms).then([&counter] {
                    bool was_parallel = counter != 0;
                    --counter;
                    return was_parallel;
                });
            });
        };
        auto running_alone = [&sm, &counter] (int instance) {
            return with_lock(sm, [&counter] {
                BOOST_REQUIRE_EQUAL(counter, 0u);
                ++counter;
                return sleep(10ms).then([&counter] {
                    --counter;
                    BOOST_REQUIRE_EQUAL(counter, 0u);
                    return true;
                });
            });
        };
        auto run = [running_in_parallel, running_alone] (int instance) {
            if (instance % 9 == 0) {
                return running_alone(instance);
            } else {
                return running_in_parallel(instance);
            }
        };
        return map_reduce(std::views::iota(0, 100), run, false, std::bit_or<bool>()).then([&counter] (bool result) {
            BOOST_REQUIRE_EQUAL(result, true);
            BOOST_REQUIRE_EQUAL(counter, 0u);
        });
    });
}


SEASTAR_TEST_CASE(test_with_semaphore) {
    return do_with(semaphore(1), 0, [] (semaphore& sem, int& counter) {
        return with_semaphore(sem, 1, [&counter] {
            ++counter;
        }).then([&counter, &sem] () {
            return with_semaphore(sem, 1, [&counter] {
                ++counter;
                throw 123;
            }).then_wrapped([&counter] (auto&& fut) {
                BOOST_REQUIRE_EQUAL(counter, 2);
                BOOST_REQUIRE(fut.failed());
                fut.ignore_ready_future();
            });
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_valid_splitting) {
    auto sm = semaphore(2);
    auto units = get_units(sm, 2, 1min).get();
    {
        BOOST_REQUIRE_EQUAL(units.count(), 2);
        BOOST_REQUIRE_EQUAL(sm.available_units(), 0);
        auto split = units.split(1);
        BOOST_REQUIRE_EQUAL(sm.available_units(), 0);
    }
    BOOST_REQUIRE_EQUAL(sm.available_units(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_invalid_splitting) {
    auto sm = semaphore(2);
    auto units = get_units(sm, 2, 1min).get();
    BOOST_REQUIRE_EQUAL(sm.available_units(), 0);
    BOOST_REQUIRE_THROW(units.split(10), std::invalid_argument);
    BOOST_REQUIRE_EQUAL(sm.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_return_when_destroyed) {
    auto sm = semaphore(3);
  {
    auto units = get_units(sm, 3, 1min).get();
    BOOST_REQUIRE_EQUAL(units.count(), 3);
    BOOST_REQUIRE_EQUAL(sm.available_units(), 0);
    BOOST_REQUIRE_EQUAL(units.return_units(1), 2);
    BOOST_REQUIRE_EQUAL(units.count(), 2);
    BOOST_REQUIRE_EQUAL(sm.available_units(), 1);
  }
    BOOST_REQUIRE_EQUAL(sm.available_units(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_return_all) {
    auto sm = semaphore(3);
    auto units = get_units(sm, 2, 1min).get();
    BOOST_REQUIRE_EQUAL(sm.available_units(), 1);
    BOOST_REQUIRE_THROW(units.return_units(10), std::invalid_argument);
    BOOST_REQUIRE_EQUAL(sm.available_units(), 1);
    units.return_all();
    BOOST_REQUIRE_EQUAL(units.count(), 0);
    BOOST_REQUIRE_EQUAL(sm.available_units(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_try_get_units) {
    constexpr size_t initial_units = 1;
    auto sm = semaphore(initial_units);

    auto opt_units = try_get_units(sm, 1);
    BOOST_REQUIRE(opt_units);

    auto opt_units2 = try_get_units(sm, 1);
    BOOST_REQUIRE(!opt_units2);

    opt_units.reset();
    BOOST_REQUIRE_EQUAL(sm.available_units(), initial_units);

    opt_units = try_get_units(sm, 1);
    BOOST_REQUIRE(opt_units);

    opt_units->return_all();
    BOOST_REQUIRE_EQUAL(opt_units->count(), 0);
    BOOST_REQUIRE_EQUAL(sm.available_units(), initial_units);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_abort) {
    auto sm = semaphore(3);
    auto units = get_units(sm, 3, 1min).get();
    BOOST_REQUIRE_EQUAL(units.count(), 3);

    abort_source as;

    auto f = get_units(sm, 1, as);
    BOOST_REQUIRE(!f.available());

    (void)sleep(1ms).then([&as] {
        as.request_abort();
    });

    BOOST_REQUIRE_THROW(f.get(), semaphore_aborted);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_units_bool_operator) {
    auto sem = semaphore(2);
    semaphore_units u0;
    BOOST_REQUIRE(!bool(u0));

    u0 = get_units(sem, 2).get();
    BOOST_REQUIRE(bool(u0));

    u0.return_units(1);
    BOOST_REQUIRE(bool(u0));

    auto n = u0.release();
    BOOST_REQUIRE(!bool(u0));
    sem.signal(n);

    u0 = get_units(sem, 2).get();
    BOOST_REQUIRE(bool(u0));
    auto u1 = std::move(u0);
    BOOST_REQUIRE(bool(u1));
    BOOST_REQUIRE(!bool(u0));

    u1.return_all();
    BOOST_REQUIRE(!bool(u1));

    u0 = get_units(sem, 2).get();
    BOOST_REQUIRE(bool(u0));
    u1 = u0.split(1);
    BOOST_REQUIRE(bool(u1));
    BOOST_REQUIRE(bool(u0));

    u0.adopt(std::move(u1));
    BOOST_REQUIRE(!bool(u1));
    BOOST_REQUIRE(bool(u0));
}

SEASTAR_THREAD_TEST_CASE(test_named_semaphore_error) {
    auto sem = make_lw_shared<named_semaphore>(0, named_semaphore_exception_factory{"name_of_the_semaphore"});
    auto check_result = [sem] (future<> f) {
        try {
            f.get();
            BOOST_FAIL("Expecting an exception");
        } catch (broken_named_semaphore& ex) {
            BOOST_REQUIRE_NE(std::string(ex.what()).find("name_of_the_semaphore"), std::string::npos);
        } catch (...) {
            BOOST_FAIL("Expected an instance of broken_named_semaphore with proper semaphore name");
        }
        return make_ready_future<>();
    };
    auto ret = sem->wait().then_wrapped(check_result);
    sem->broken();
    sem->wait().then_wrapped(check_result).then([ret = std::move(ret)] () mutable {
        return std::move(ret);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_named_semaphore_timeout) {
    auto sem = make_lw_shared<named_semaphore>(0, named_semaphore_exception_factory{"name_of_the_semaphore"});

    auto f = sem->wait(named_semaphore::clock::now() + 1ms, 1);
    try {
        f.get();
        BOOST_FAIL("Expecting an exception");
    } catch (named_semaphore_timed_out& ex) {
        BOOST_REQUIRE_NE(std::string(ex.what()).find("name_of_the_semaphore"), std::string::npos);
    } catch (...) {
        BOOST_FAIL("Expected an instance of named_semaphore_timed_out with proper semaphore name");
    }
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_abort_after_wait) {
    auto sem = semaphore(0);
    abort_source as;
    int x = 0;
    auto fut1 = sem.wait(as).then([&x] {
        x++;
    });
    as.request_abort();
    sem.signal();
    BOOST_CHECK_THROW(fut1.get(), semaphore_aborted);
    BOOST_REQUIRE_EQUAL(x, 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_abort_with_exception_after_wait) {
    auto sem = semaphore(0);
    abort_source as;
    int x = 0;
    auto fut1 = sem.wait(as).then([&x] {
        x++;
    });
    as.request_abort_ex(expected_exception());
    sem.signal();
    BOOST_CHECK_THROW(fut1.get(), expected_exception);
    BOOST_REQUIRE_EQUAL(x, 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_abort_before_wait) {
    auto sem = semaphore(0);
    abort_source as;
    int x = 0;
    as.request_abort();
    auto fut1 = sem.wait(as).then([&x] {
        x++;
    });
    sem.signal();
    BOOST_CHECK_THROW(fut1.get(), semaphore_aborted);
    BOOST_REQUIRE_EQUAL(x, 0);
}

SEASTAR_THREAD_TEST_CASE(test_reassigned_units_are_returned) {
    auto sem0 = semaphore(1);
    auto sem1 = semaphore(1);
    auto units = get_units(sem0, 1).get();
    auto wait = sem0.wait(1);
    BOOST_REQUIRE(!wait.available());
    units = get_units(sem1, 1).get();
    timer t([] { abort(); });
    t.arm(1s);
    // will hang if units are not returned when reassigned
    wait.get();
    t.cancel();
}

SEASTAR_THREAD_TEST_CASE(test_get_units_after_move) {
    auto sem = std::make_unique<semaphore>([] { return semaphore(0); }());
    auto f = get_units(*sem, 1);
    BOOST_REQUIRE(!f.available());
    sem->signal();
    f.get();
}

SEASTAR_THREAD_TEST_CASE(test_get_units_abort_after_move) {
    auto sem = std::make_unique<semaphore>([] { return semaphore(0); }());
    abort_source as;
    auto f = get_units(*sem, 1, as);
    BOOST_REQUIRE(!f.available());
    as.request_abort();

    BOOST_REQUIRE_THROW(f.get(), abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_immediate_abort_after_move) {
    auto sem = std::make_unique<semaphore>([] { return semaphore(0); }());
    abort_source as;
    as.request_abort();

    BOOST_REQUIRE_THROW(get_units(*sem, 1, as).get(), abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_get_units_after_move_assign) {
    auto sem = semaphore(0);
    sem = [] { return semaphore(0); }();
    auto f = get_units(sem, 1);
    BOOST_REQUIRE(!f.available());
    sem.signal();
    f.get();
}

SEASTAR_THREAD_TEST_CASE(test_get_units_abort_after_move_assign) {
    auto sem = semaphore(0);
    sem = [] { return semaphore(0); }();
    abort_source as;
    auto f = get_units(sem, 1, as);
    BOOST_REQUIRE(!f.available());
    as.request_abort();

    BOOST_REQUIRE_THROW(f.get(), abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_immediate_abort_after_move_assign) {
    auto sem = semaphore(0);
    sem = [] { return semaphore(0); }();
    abort_source as;
    as.request_abort();

    BOOST_REQUIRE_THROW(get_units(sem, 1, as).get(), abort_requested_exception);
}

namespace {

// A minimal shared pool of units that semaphores can borrow from, used by the
// tests below.  It only tracks how many units it has left to lend and how many
// times it was told that units came back.
class test_borrow_pool final : public semaphore_borrow_source {
    ssize_t _available;
    ssize_t _total;
    unsigned _repaid_calls = 0;
public:
    explicit test_borrow_pool(ssize_t available) noexcept : _available(available), _total(available) {}

    // semaphore_borrow_source
    ssize_t available() const noexcept override { return _available; }
    ssize_t lendable_total() const noexcept override { return _total; }
    void borrow(ssize_t n) noexcept override { _available -= n; }
    void repay(ssize_t n) noexcept override { _available += n; }
    void on_repaid() noexcept override { ++_repaid_calls; }

    // test helpers
    void add(ssize_t n) noexcept { _available += n; _total += n; }
    // Pretend another borrower took `n` units, leaving the total alone.
    void lend_out(ssize_t n) noexcept { _available -= n; }
    void set_total(ssize_t n) noexcept { _available += n - _total; _total = n; }
    unsigned repaid_calls() const noexcept { return _repaid_calls; }
};

} // anonymous namespace

SEASTAR_TEST_CASE(test_semaphore_borrow_only_the_shortfall) {
    test_borrow_pool pool(100);
    semaphore sem(10);
    sem.set_borrow_source(&pool);
    BOOST_REQUIRE(sem.borrow_source() == &pool);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);

    // 10 of the 50 units come out of our own counter, only 40 are borrowed.
    BOOST_REQUIRE(sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 60);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 0u);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_semaphore_borrow_repays_before_keeping_units) {
    test_borrow_pool pool(100);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

    BOOST_REQUIRE(sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 60);

    // Returned units repay the pool first; we keep only our own 10 back.
    sem.signal(50);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_semaphore_borrow_surplus_is_not_hoarded) {
    test_borrow_pool pool(100);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

    BOOST_REQUIRE(sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);

    // Even a partial return goes to the pool before it stays with us: the 45
    // units returned repay all 40 borrowed ones and leave 5 behind.
    sem.signal(45);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 5);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);
    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_waiter_reports_its_demand) {
    test_borrow_pool pool(0);
    semaphore sem(2);
    sem.set_borrow_source(&pool);

    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 0u);

    // Nothing to borrow, so the request has to queue.
    auto f = sem.wait(5);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 5u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 0);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 2u);

    // Our own units are enough once they arrive; nothing gets borrowed.
    sem.signal(3);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 0u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_nudge_admits_queued_waiter) {
    test_borrow_pool pool(0);
    semaphore sem(0);
    sem.set_borrow_source(&pool);

    auto f = sem.wait(5);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 5u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);

    // The pool gaining units does not by itself wake the waiter.
    pool.add(10);
    BOOST_REQUIRE(!f.available());

    // A zero-delta adjust_capacity() re-evaluates the queue and borrows for it.
    sem.adjust_capacity(0);
    BOOST_REQUIRE(f.available());
    f.get();
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 5);
    BOOST_REQUIRE_EQUAL(pool.available(), 5);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_does_not_barge) {
    test_borrow_pool pool(0);
    semaphore sem(0);
    sem.set_borrow_source(&pool);

    auto f = sem.wait(5);
    BOOST_REQUIRE(!f.available());

    // The pool could fund a newcomer, but the queued waiter keeps its place.
    pool.add(100);
    BOOST_REQUIRE(!sem.try_wait(1));
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);

    // With the queue drained, borrowing is allowed again.
    sem.adjust_capacity(0);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 5);
    BOOST_REQUIRE(sem.try_wait(1));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 6);
    BOOST_REQUIRE_EQUAL(pool.available(), 94);
}

SEASTAR_TEST_CASE(test_semaphore_borrow_growth_repays) {
    test_borrow_pool pool(100);
    semaphore sem(10);
    sem.set_borrow_source(&pool);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 10u);

    BOOST_REQUIRE(sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 60);

    // Growing our own capacity by the borrowed amount hands the units back.
    sem.adjust_capacity(40);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 50u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);

    // A smaller growth repays only as much as our capacity now covers.
    BOOST_REQUIRE(sem.try_wait(20));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 20);
    BOOST_REQUIRE_EQUAL(pool.available(), 80);
    sem.adjust_capacity(5);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 55u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 15);
    BOOST_REQUIRE_EQUAL(pool.available(), 85);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_shrink) {
    test_borrow_pool pool(0);
    semaphore sem(20);
    sem.set_borrow_source(&pool);

    BOOST_REQUIRE_EQUAL(sem.capacity(), 20u);

    // More than our own capacity and an empty pool, so the request queues.
    auto f = sem.wait(30);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 30u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);

    pool.add(100);
    BOOST_REQUIRE(!f.available());

    // Shrinking drives the counter down, but the waiter is still satisfiable
    // (15 of our own units plus 15 borrowed) so it is admitted, not parked.
    sem.adjust_capacity(-5);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.capacity(), 15u);
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 15);
    BOOST_REQUIRE_EQUAL(pool.available(), 85);

    // Shrinking further may drive the counter negative; borrowed units cannot
    // be repaid while it is.
    sem.adjust_capacity(-10);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 5u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -10);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 15);
    BOOST_REQUIRE_EQUAL(pool.available(), 85);

    // Returning the units repays the pool and leaves our shrunk capacity of 5.
    sem.signal(30);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 5);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_broken_repays) {
    test_borrow_pool pool(40);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

    BOOST_REQUIRE(sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 0);

    auto f = sem.wait(1);
    BOOST_REQUIRE(!f.available());

    // Breaking the semaphore must not lose the pool's units.
    sem.broken();
    BOOST_REQUIRE_THROW(f.get(), broken_semaphore);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 40);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);

    // The repaid units stay in the pool: a broken semaphore cannot borrow.
    BOOST_REQUIRE(!sem.try_wait(1));
    BOOST_REQUIRE_THROW(sem.wait(1).get(), broken_semaphore);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 40);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_without_borrow_source) {
    test_borrow_pool pool(100);
    semaphore sem(3);

    BOOST_REQUIRE(sem.borrow_source() == nullptr);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 0u);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 3u);

    BOOST_REQUIRE(sem.try_wait(3));
    BOOST_REQUIRE(!sem.try_wait(1));
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);

    auto f = sem.wait(2);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 2u);

    sem.signal(3);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.available_units(), 1);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);

    // Attaching and then detaching a source leaves the semaphore as it was.
    sem.set_borrow_source(&pool);
    BOOST_REQUIRE(sem.borrow_source() == &pool);
    sem.set_borrow_source(nullptr);
    BOOST_REQUIRE(sem.borrow_source() == nullptr);
    BOOST_REQUIRE(!sem.try_wait(100));
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 1);
}

SEASTAR_TEST_CASE(test_semaphore_borrow_shared_pool) {
    test_borrow_pool pool(10);
    semaphore a(0);
    semaphore b(0);
    a.set_borrow_source(&pool);
    b.set_borrow_source(&pool);

    BOOST_REQUIRE(a.try_wait(10));
    BOOST_REQUIRE_EQUAL(a.borrowed(), 10);
    BOOST_REQUIRE_EQUAL(pool.available(), 0);

    // The pool is drained, so the other semaphore gets nothing.
    BOOST_REQUIRE(!b.try_wait(1));
    BOOST_REQUIRE_EQUAL(b.borrowed(), 0);

    a.signal(10);
    BOOST_REQUIRE_EQUAL(a.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(a.available_units(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 10);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);

    // Once repaid, the units are there for the other borrower.
    BOOST_REQUIRE(b.try_wait(1));
    BOOST_REQUIRE_EQUAL(b.borrowed(), 1);
    BOOST_REQUIRE_EQUAL(pool.available(), 9);
    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_units_repay_on_release) {
    test_borrow_pool pool(100);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

  {
    auto units = get_units(sem, 50).get();
    BOOST_REQUIRE_EQUAL(units.count(), 50u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 60);

    // Returning part of the units repays the pool before we keep any.
    BOOST_REQUIRE_EQUAL(units.return_units(45), 5u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 5);
  }
    // The destructor returns the rest.
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 1u);

    // Borrowing again through get_units() and dropping the units repays too.
  {
    auto units = get_units(sem, 30).get();
    BOOST_REQUIRE_EQUAL(units.count(), 30u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 20);
    BOOST_REQUIRE_EQUAL(pool.available(), 80);
  }
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 2u);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_is_all_or_nothing) {
    test_borrow_pool pool(20);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

    // The shortfall is 40 but the pool holds only 20.  Taking those 20 could not
    // admit the request anyway, so the pool is left alone.
    BOOST_REQUIRE(!sem.try_wait(50));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 20);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);

    // Same on the blocking path: a queued waiter sits on no borrowed units.
    auto f = sem.wait(50);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 20);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);

    // So the units are still there for a borrower that can use them.
    pool.add(20);
    sem.adjust_capacity(0);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available(), 0);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_borrow_broken_cannot_borrow) {
    test_borrow_pool pool(100);
    semaphore sem(0);
    sem.set_borrow_source(&pool);
    sem.broken();

    // A funded pool must not let a broken semaphore hand out units.
    BOOST_REQUIRE(!sem.try_wait(5));
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);

    BOOST_REQUIRE_THROW(sem.wait(5).get(), broken_semaphore);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 100);
    BOOST_REQUIRE_EQUAL(pool.repaid_calls(), 0u);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_capacity) {
    semaphore sem(30);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 30u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 30);

    // set_capacity() moves the counter along with the capacity ...
    sem.set_capacity(10);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 10u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);

    // ... and so does adjust_capacity(), in either direction.
    sem.adjust_capacity(5);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 15u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 15);

    sem.adjust_capacity(-15);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);

    // Units in flight are not affected by a resize: the counter absorbs the
    // delta and the outstanding units come back on top of it.
    sem.set_capacity(10);
    BOOST_REQUIRE(sem.try_wait(10));
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
    sem.set_capacity(4);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 4u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -6);
    sem.signal(10);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 4u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 4);

    // Resizing re-evaluates the queue, so growing admits whoever now fits.
    auto f = sem.wait(6);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 6u);
    sem.set_capacity(6);
    f.get();
    BOOST_REQUIRE_EQUAL(sem.capacity(), 6u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_admit_oversized_when_idle) {
    test_borrow_pool pool(10);
    semaphore sem(10);
    sem.set_borrow_source(&pool);

    // 1000 units are out of reach: 10 of our own plus 10 in the pool.
    auto f = sem.wait(1000);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 1000u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 10);

    // Enabling the escape admits it right away, because nothing is outstanding.
    sem.set_admit_oversized_when_idle(true);
    BOOST_REQUIRE(f.available());
    f.get();
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -990);
    // It was admitted regardless, so there was no point borrowing for it.
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 10);

    // At most one at a time: the next oversized waiter blocks while this is out.
    auto g = sem.wait(1000);
    BOOST_REQUIRE(!g.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 1000u);

    // It is admitted once the first request gives its units back.
    sem.signal(1000);
    BOOST_REQUIRE(g.available());
    g.get();
    BOOST_REQUIRE_EQUAL(sem.available_units(), -990);

    sem.signal(1000);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 10);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 10u);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 10);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_admit_oversized_shrink_liveness) {
    test_borrow_pool pool(0);
    semaphore sem(100);
    sem.set_borrow_source(&pool);
    sem.set_admit_oversized_when_idle(true);

    BOOST_REQUIRE(sem.try_wait(60));
    auto f = sem.wait(40);
    BOOST_REQUIRE(f.available());               // fits in what is left
    f.get();
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);

    // Nothing left and an empty pool, so this queues.
    auto g = sem.wait(50);
    BOOST_REQUIRE(!g.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 50u);

    // Shrinking puts 50 permanently out of reach, but 100 units are still out
    // and may yet come back, so the waiter must not be admitted early.
    sem.set_capacity(20);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 20u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -80);
    BOOST_REQUIRE(!g.available());
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);

    // Once everything is back the request can never be funded, so it is let
    // through instead of being parked forever.  This is what keeps a resize
    // from deadlocking a waiter.
    sem.signal(100);
    BOOST_REQUIRE(g.available());
    g.get();
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -30);

    sem.signal(50);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 20);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_oversized_blocks_by_default) {
    semaphore sem(10);
    auto f = sem.wait(1000);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 1000u);

    // Signalling units it can still never gather does not admit it ...
    sem.signal(500);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.available_units(), 510);

    // ... and neither does a resize, as long as the escape is off.
    sem.set_capacity(100);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 100u);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.waiters(), 1u);

    // Opting in is what unblocks it.
    sem.set_admit_oversized_when_idle(true);
    BOOST_REQUIRE(f.available());
    f.get();
    BOOST_REQUIRE_EQUAL(sem.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), -400);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_adjust_capacity_clamps_the_delta) {
    semaphore sem(8);

    // A delta larger than the capacity is clamped, so the counter and the
    // capacity stay in step rather than drifting apart by the clamped amount.
    sem.adjust_capacity(-100);
    BOOST_REQUIRE_EQUAL(sem.capacity(), 0u);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);

    // Which means an idle semaphore still looks idle afterwards.
    sem.set_admit_oversized_when_idle(true);
    BOOST_REQUIRE(sem.wait(1000).available());
    BOOST_REQUIRE_EQUAL(sem.available_units(), -1000);
    sem.signal(1000);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_semaphore_escape_becomes_eligible_when_source_shrinks) {
    test_borrow_pool pool(500);
    semaphore sem(200);
    sem.set_borrow_source(&pool);
    sem.set_admit_oversized_when_idle(true);

    // Another borrower holds the whole pool, but it could still be repaid, so a
    // request for less than capacity + the pool's total waits for it.
    pool.lend_out(500);
    auto f = sem.wait(387);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(sem.next_waiter_units(), 387u);

    // The pool shrinks to nothing. 387 can never be funded now, so the escape
    // applies - but the semaphore does not watch the source, so nothing happens
    // until someone asks it to look again. Whoever shrinks the source owes the
    // borrowers that nudge.
    pool.set_total(0);
    BOOST_REQUIRE(!f.available());

    sem.adjust_capacity(0);
    BOOST_REQUIRE(f.available());
    f.get();
    BOOST_REQUIRE_EQUAL(sem.available_units(), -187);
    BOOST_REQUIRE_EQUAL(sem.borrowed(), 0);
    sem.signal(387);
    BOOST_REQUIRE_EQUAL(sem.available_units(), 200);
}
