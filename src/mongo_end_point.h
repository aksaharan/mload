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

#include <deque>
#include <memory>
#include <unordered_map>
#include "mongo_cxxdriver.h"
#include "mongo_cluster.h"
#include "mongo_operations.h"
#include "threading.h"

namespace cpp {
    namespace mtools {

        /*
         * The settings for an MongoEndPoint.  The settings aren't a template so they are a separate.
         */
        struct MongoEndPointSettings {
            bool startImmediate;
            bool directLoad;
            size_t queueSize;
            size_t threadCount;
            size_t sleepTime;
        };

        /**
         * MongoDB end point (i.e. a mongoD or mongoD).
         * There is no associated queue.  That is a template parameter
         * This class handles getting operations from a queue and then using N threads to send the
         * operations to mongo as quickly as possible.
         */
        template<typename TOpQueue = cpp::mtools::OpQueueNoLock>
        class BasicMongoEndPoint {
        public:
            BasicMongoEndPoint(MongoEndPointSettings settings, std::string connection) :
                    _threadPool(settings.threadCount),
                    _connection(std::move(connection)),
                    _opQueue(settings.queueSize),
                    _sleepTime(settings.sleepTime),
                    _threadCount(settings.threadCount)
            {
                assert(settings.threadCount);
                if (settings.startImmediate) start();
            }
            ~BasicMongoEndPoint() {
                shutdown();
            }

            /**
             * @return the connection string to this end point
             */
            const std::string& connection() {
                return _connection;
            }

            /**
             * @return are threads active
             */
            bool isRunning() {
                return _threadPool.size();
            }

            /**
             * Starts the threads running for inserts
             * It is desirable to delay this until there is data in the queue if the threads are spinning
             */
            void start() {
                assert(!isRunning());
                for (size_t i = 0; i < _threadCount; ++i)
                    _threadPool.queue([this] () {this->run();});
            }

            /**
             * Shutdown the queue after it is cleared
             */
            void gracefulShutdown() {
                _threadPool.endWaitInitiate();
            }

            /**
             * Shutsdown the queue after it is cleared and waits for the threads to be joined
             */
            void gracefulShutdownJoin() {
                _threadPool.endWaitInitiate();
                joinAll();
            }

            /**
             * Immediately shutdown the queue
             */
            void shutdown() {
                _threadPool.terminateInitiate();
            }

            /**
             * Wait for all threads to be joined
             * Should NOT be called on it's own, this will NOT stop the threads
             */
            void joinAll() {
                _threadPool.joinAll();
            }

            /**
             * Push onto the thread queue
             */
            bool push(DbOpPointer dbOp) {
                //TODO: figure out error handling, or change function, problem is that the data is consumed even on false
                assert(_opQueue.push(dbOp));
                return true;
            }

            /**
             * thread work loop
             *
             */
            void run() {
                DbOpPointer currentOp;
                mongo::ScopedDbConnection conn(_connection);
                //Discount the first miss as the loop is probably starting dry
                bool miss = false;
                bool firstmiss = true;
                size_t missCount {};
                while (!_threadPool.terminate()) {
                    if (pop(currentOp)) {
                        if (miss) {
                            miss = false;
                            firstmiss = false;
                            std::cout << _connection << ": Hitting" << std::endl;
                        }
                        currentOp->run(conn);
                    }
                    else {
                        if (_threadPool.endWait()) break;
                        //TODO: log levels.  If you are seeing misses std::cout is cheap
                        if (!miss && !firstmiss) {
                            std::cout << _connection << ": Missing" << std::endl;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(_sleepTime));
                        miss = true;
                        if (!firstmiss) ++missCount;
                    }
                }
                if (missCount) std::cout << "Endpoint misses: " << missCount << ".  Slept: "
                                         << missCount * _sleepTime / 1000 << " seconds"
                                         << std::endl;
            }

        private:
            bool pop(DbOpPointer& dbOp) {
                return _opQueue.pop(dbOp);
            }

            cpp::ThreadPool _threadPool;
            std::string _connection;
            TOpQueue _opQueue;
            size_t _sleepTime;
            size_t _threadCount;
        };

        /**
         * Holds end points to a cluster.
         * All endpoints should either be monogS or mongoD
         */
        template<typename TOpQueue = cpp::mtools::OpQueueNoLock>
        class MongoEndPointHolder {
        public:
            using MongoEndPoint = BasicMongoEndPoint<TOpQueue>;
            using MongoEndPointPtr = std::unique_ptr<MongoEndPoint>;
            //Note that ShardName can also be a mongoS, but in that case it doesn't much matter
            using MongoEndPointMap = std::unordered_map<cpp::mtools::MongoCluster::ShardName, MongoEndPointPtr>;

            MongoEndPointHolder(MongoEndPointSettings settings, MongoCluster& mCluster) :
                    _started( false)
            {
                if (settings.directLoad) {
                    for (auto& shard : mCluster.shards())
                        _epm.emplace(std::make_pair(shard.first,
                                                    MongoEndPointPtr(new MongoEndPoint {settings,
                                                                                        shard.second})));
                }
                else {
                    for (auto& mongoS : mCluster.mongos())
                        _epm.emplace(std::make_pair(mongoS, MongoEndPointPtr(new MongoEndPoint {
                                settings, mongoS})));
                }
                assert(_epm.size());
                //We like to start with edge cases
                _cycleItr = _epm.begin();
                if (settings.startImmediate) start();
            }

            /**
             * @return MongoEndPoint for a specific shard/mongoS (though shouldn't need to be called
             * in the mongoS case)
             */
            MongoEndPoint* at(const cpp::mtools::MongoCluster::ShardName& shard) {
                return _epm.at(shard).get();
            }

            /**
             * Hand out end points in a round robin, use for mongoS
             */
            MongoEndPoint* getMongoSCycle() {
                cpp::MutexLockGuard lock(_cycleMutex);
                ++_cycleItr;
                if (_cycleItr == _epm.end()) _cycleItr = _epm.begin();
                return _cycleItr->second.get();
            }

            /**
             * Are the end points active?
             */
            bool isRunning() {
                return _started;
            }

            /**
             * Start up all end points
             * It is desirable to delay this while there is no data if the end points don't wait
             */
            void start() {
                _started = true;
                for (auto& i : _epm)
                    i.second->start();
            }

            /**
             * Have all of the end points shutdown when their queues are cleared, join those threads.
             */
            void gracefulShutdownJoin() {
                for (auto& ep : _epm)
                    ep.second->gracefulShutdownJoin();
            }

        private:
            cpp::Mutex _cycleMutex;
            typename MongoEndPointMap::iterator _cycleItr;
            MongoEndPointMap _epm;bool _started;

        };

    }  //namespace mtools
}  //namespace cpp
