/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#ifndef THREADING_H_
#define THREADING_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace cpp {


typedef             std::mutex                  Mutex;
typedef             std::lock_guard<std::mutex> MutexLockGuard;
typedef             std::function<void()>       ThreadFunction;
typedef             std::condition_variable     ConditionVariable;
typedef             std::unique_lock<std::mutex> MutexUniqueLock;



//TODO: refactor everything to PIMPL
class ThreadPool;


class ThreadPoolWorker {
public:
    ThreadPoolWorker(ThreadPool &pool) : _pool(pool) {}

    void operator()();

private:
    ThreadPool &_pool;
};

class ThreadPool {
public:
    ThreadPool(size_t size): _terminate(false), _endWait(false) {
        do {
            _threads.push_back(std::thread(ThreadPoolWorker(*this)));
        } while (--size);
    }
    ~ThreadPool() {
        _terminate = true;
        _workNotify.notify_all();
        joinAll();

    }

    void queue(ThreadFunction func) {
        MutexLockGuard lock(_workMutex);
        _workQueue.push_back(func);
        _workNotify.notify_one();
    }

    void joinAll() {
        for(auto &thread: _threads)
            if(thread.joinable())
                thread.join();
    }

    bool terminate() { return _terminate; }
    bool endWait() { return _endWait; }

    void terminateInitiate() { _terminate = true; _endWait = true; _workNotify.notify_all(); }
    void endWaitInitiate() { _endWait = true; _workNotify.notify_all(); }

    size_t size() const { return _workQueue.size(); }


private:
    friend class ThreadPoolWorker;
    void _workLoop();

    std::atomic<bool> _terminate;
    std::atomic<bool> _endWait;

    std::deque<std::thread> _threads;
    Mutex _workMutex;
    ConditionVariable _workNotify;
    std::deque<ThreadFunction> _workQueue;
};

inline void ThreadPoolWorker::operator()() {
    _pool._workLoop();
}

template <typename T, typename H = std::deque<T>>
    class RoundRobin {
    public:
        using Container = H;
        using Value = T;

        template <typename... Args>
        RoundRobin(Args ...args) : _container(args...) {
            reset();
        }

        RoundRobin(Container &&container) : _container(std::forward<Container>(container)) {
            reset();
        }

        bool next(Value *ret) const {
            cpp::MutexLockGuard lock(_mutex);

            if(_container.empty())
                return false;
            assert(_container.size());
            if(++_position == _container.end())
                _position = _container.begin();
           *ret = *_position;
           return true;
        }

        //TODO: Make this more robust, i.e. dump reset
        void remove(const Value& value) {
            cpp::MutexLockGuard lock(_mutex);
            _container.erase(std::remove(_container.begin(), _container.end(), value), _container.end());
            reset();
        }

    private:
        mutable Container _container;
        mutable typename Container::iterator _position;
        mutable cpp::Mutex _mutex;

        void reset() {
            _position = _container.begin();
        }
    };


} //namespace cpp

#endif /* THREADING_H_ */
