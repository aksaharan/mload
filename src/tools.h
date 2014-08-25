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

#pragma once

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <string>
#ifdef __linux__
#include <unistd.h>
#endif
#ifdef __WIN32__
#include <windows.h>
#endif

namespace cpp {

#ifdef __linux__
    inline size_t getTotalSystemMemory() {
        long pages = sysconf( _SC_PHYS_PAGES);
        long page_size = sysconf( _SC_PAGE_SIZE);
        return pages * page_size;
    }
#endif

    //TODO: rest of windows support
#ifdef __WIN32__
#include <windows.h>
    inline size_t getTotalSystemMemory()
    {
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        return status.ullTotalPhys;
    }
#endif

    /**
     * Sfinae true/false
     */
    struct SfinaeTypes {
        typedef char one;
        typedef struct {
            char arr[2];
        } two;
    };

    /*
     * Checks if a class tree has a shift left operator, e.g. <<
     */
    template<typename T>
    class HasShiftLeftImpl : public SfinaeTypes {
        struct BaseMixin {
            friend std::ostream& operator<<(T t, std::ostream &o) {
                return o;
            }
        };

        struct Base : public T, public BaseMixin {
        };
        template<typename H, H h> class Helper {
        };
        template<typename U>
        static constexpr two deduce(U*, Helper<void (BaseMixin::*)(), &U::operator<<>* = 0);
        static constexpr one deduce(...);

    public:
        static constexpr bool value = sizeof(one) == sizeof(deduce((Base*) (0)));
    };

    template<typename T>
    class HasShiftLeft : public HasShiftLeftImpl<T> {
    };

    /**
     * Simple timer
     */
    template<typename T = std::chrono::high_resolution_clock>
    class SimpleTimer {
    public:
        using TimePoint = typename T::time_point;

        TimePoint startTime, endTime;

        void start() {
            startTime = T::now();
        }
        void stop() {
            endTime = T::now();
        }
        long seconds() {
            return std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
        }

        long nanos() {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
        }

        long millis() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        }

        template<typename Td = std::chrono::nanoseconds>
        Td duration() {
            return std::chrono::duration_cast<Td>(endTime - startTime);
        }
    };

    /**
     * Lap timer
     */
    template<typename T = std::chrono::high_resolution_clock,
            template<typename, typename > class Tc = std::deque>
    class LapTimer {
    public:
        using TimePoint = typename T::time_point;
        using ContainerType = Tc<T, std::allocator<T>>;

        TimePoint checkTime;
        ContainerType laps;

        void start() {
            checkTime = T::now();
        }

        void lap() {
            TimePoint end = T::now();
            laps.emplace_back(end - checkTime);
            checkTime = end;
        }

        void reset() {
            laps.clear();
            start();
        }

        void reserver(size_t size) {
            laps.reserve(size);
        }
        //template < typename U = std::chrono::nanoseconds >
        long duration() {
            TimePoint total = std::accumulate(std::begin(laps), std::end(laps), TimePoint {});
            return std::chrono::duration_cast<std::chrono::nanoseconds>(total).count();
        }

        //template < typename U = std::chrono::nanoseconds>
        unsigned long long avg() {
            if (laps.empty()) return 0;
            return duration() / laps.size();
            //return duration<U>() / laps.size();
        }

    };

    /**
     * Event timer.  Stores an event along with the time it occured
     */
    template<typename E>
    class EventTimer {
    public:
        using EventTime = std::tuple<std::chrono::high_resolution_clock::time_point, E>;
        std::deque<EventTime> events;

        void insertEvent(E event) {
            EventTime inter = EventTime(std::chrono::high_resolution_clock::now(), event);
            events.push_back(inter);
        }
    };

    /*
     * Arg 2 is the ceiling.  If Arg1 exceeds the ceiling the ceiling is returned using Arg1's type
     */
    template<typename T, typename U>
    T SetCeiling(const T &t, const U &u) {
        return t > u ? T(u) : t;
    }

    /*
     * Disk reference types
     */
    using LogicalDiskMapping = std::vector<std::string>;

    //TODO: make LocSegment a file name and start/end (64 bit ints)
    using LocSegment = std::string;
    using LocSegMapping = std::vector<cpp::LocSegment>;
    using LogicalLoc = size_t;

    /*
     * DocLoc
     * location = logical location
     * start = start byte, inclusive
     * end = end byte, exclusive
     * [start,end)
     */
    struct DocLoc {
        static_assert(sizeof(std::streamsize) >= sizeof(std::int64_t), "mload assumes that std::streamsize is at least 64 bits so it can handle any files size");
        LogicalLoc location;
        std::streamsize start;
        //Max bson size is currently 16Megs so int covers this easily.
        int length;
    };

    template<typename Tp, typename ... Args>
    inline std::unique_ptr<Tp> make_unique(Args ...args) {
        return std::unique_ptr<Tp>(new Tp(std::forward<Args>(args)...));
    }
}  //namespace cpp
