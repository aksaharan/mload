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

#ifndef CONCURRENTCONTAINER_H_
#define CONCURRENTCONTAINER_H_

#include <algorithm>
#include <deque>
#include <functional>
#include <type_traits>
#include "threading.h"

namespace cpp {


    template <typename _Tp, template <typename, typename> class _Tc>
    class BasicConcurrentQueue {
    public:
        typedef             _Tp                             value_type;
        typedef             value_type                      ValueType;
        typedef             _Tc<_Tp, std::allocator<_Tp>>   ContainerType;

        //TODO: check for move or copy construct and do w/e can be done.
        static_assert(std::is_constructible<value_type, typename ContainerType::value_type>::value, "Cannot copy construct return value from container value.");

        BasicConcurrentQueue(): _mutex(new Mutex),  _sizeMaxNotify(new ConditionVariable) { }

        void swap(ContainerType &from) {
            MutexLockGuard lock(*_mutex);
            _container.swap(from);
        }

        bool pop(value_type &ret) {
            MutexLockGuard lock(*_mutex);
            if(_container.empty())
                return false;
            ret = (std::move(_container.front()));
            _container.pop_front();
            _sizeMaxNotify->notify_one();
            return true;
        }

        bool popRandom(value_type &ret) {
            MutexLockGuard lock(*_mutex);
            if(_container.empty())
                return false;

            ret = (std::move(_container.front()));
            _container.pop_front();
            _sizeMaxNotify->notify_one();
            return true;
        }

        void push(value_type value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::move(value));
        }

        size_t pushGetSize(value_type value) {
            MutexLockGuard lock(*_mutex);
            _container.push_back(std::move(value));
            return _container.size();
        }

        void pushCheckMax(value_type value) {
            MutexUniqueLock lock(*_mutex);
            _container.push_back(std::move(value));
            _sizeMaxNotify->wait(lock, [this]() { return this->_sizeMax && (this->_container.size() > this->_sizeMax); });

        }

        template <typename U>
        bool popToPushBack(U *ret, size_t count = 10) {
            MutexLockGuard lock(*_mutex);
            if(_container.size() < count) {
            	if(_container.empty())
					return false;
				count = _container.size();
            }

            auto stop = _container.begin();
            std::advance(stop, count);
            for(auto i = _container.begin(); i != stop; ++i)
            	ret->push_back(std::move(*i));
            _container.erase(_container.begin(), stop);
            return true;
        }

        size_t size() const { MutexLockGuard lock(*_mutex); return _container.size(); }
        bool empty() const { return size() == 0; }

        void sizeMaxSet(size_t sizeMax) {
            bool doNotify = _sizeMax && (sizeMax > _sizeMax);
            _sizeMax = sizeMax;
            if(doNotify)
                _sizeMaxNotify->notify_all();
        };

        template <typename InputIterator>
        void moveIn(InputIterator first, InputIterator last) {
            MutexLockGuard lock(*_mutex);
            while(first != last) {
                _container.emplace_back(std::move(*first));
                ++first;
            }
        }

        template <typename U>
        void moveIn(U *u) {
            moveIn(u->begin(), u->end());
        }

        template <typename Cmp = std::less<_Tp>>
        void sort(Cmp compare) {
            MutexLockGuard lock(*_mutex);
            std::sort(_container.begin(), _container.end(), compare);
        }

        ContainerType& unSafeAccess() { return _container; }

    private:
        using iterator = typename ContainerType::iterator;
        using reverse_iterator = std::reverse_iterator<iterator>;

        ContainerType _container;
        mutable std::unique_ptr<Mutex> _mutex;
        mutable std::unique_ptr<ConditionVariable> _sizeMaxNotify;
        volatile size_t _sizeMax{};
    };


    template <typename Value, template <typename, typename> class Container = std::deque> using ConcurrentQueue = BasicConcurrentQueue<Value, Container>;

} /* namespace cpp */

#endif /* CONCURRENTCONTAINER_H_ */
