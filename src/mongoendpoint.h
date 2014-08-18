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

#ifndef MONGOENDPOINT_H_
#define MONGOENDPOINT_H_

#include <deque>
#include <memory>
#include <unordered_map>
#include "mongocxxdriver.h"
#include "mongocluster.h"
#include "mongoendpointsettings.h"
#include "opqueue.h"
#include "threading.h"

namespace cpp {
    namespace mtools {

        template <typename TOpQueue = cpp::mtools::OpQueueNoLock>
        class BasicMongoEndPoint {
        public:
            BasicMongoEndPoint(EndPointSettings settings, std::string connection) :
                _threadPool(settings.threadCount),
                _connection(std::move(connection)),
                _opQueue(settings.queueSize),
                _sleepTime(settings.sleepTime),
                _threadCount(settings.threadCount) {
                assert(settings.threadCount);
                if(settings.startImmediate)
                    start();
            }
            ~BasicMongoEndPoint() {
                shutdown();
            }

            const std::string& connection() { return _connection; }

            bool isRunning() {
                return _threadPool.size();
            }

            void start() {
                assert(!isRunning());
                for(size_t i = 0; i < _threadCount; ++i)
                    _threadPool.queue([this] () {this->run();});
            }
            void gracefulShutdown() {
                _threadPool.endWaitInitiate();
            }
            void gracefulShutdownJoin() {
                _threadPool.endWaitInitiate();
                joinAll();
            }

            void shutdown() {
                _threadPool.terminateInitiate();
            }

            void joinAll() {
                _threadPool.joinAll();
            }

            //TODO: figure out error handling, or change function, problem is that the data is consumed even on false
            bool push(DbOpPointer dbOp) {
                assert(_opQueue.push(dbOp));
                return true;
            }

            void run() {
                DbOpPointer currentOp;
                mongo::ScopedDbConnection conn(_connection);
                //Discount the first miss as the loop is probably starting dry
                bool miss = false;
                bool firstmiss = true;
                size_t missCount{};
                while(!_threadPool.terminate()) {
                    if(pop(currentOp)) {
                        if(miss) {
                            miss = false;
                            firstmiss = false;
                            std::cout << _connection << ": Hitting" << std::endl;
                        }
                        currentOp->run(conn);
                    }
                    else {
                        if(_threadPool.endWait())
                            break;
                        std::this_thread::sleep_for(std::chrono::milliseconds(_sleepTime));
                        if(!miss && !firstmiss) {
                            std::cout << _connection << ": Missing" << std::endl;
                        }
                        miss = true;
                        if(!firstmiss)
                            ++missCount;
                    }
                }
                if(missCount)
                    std::cout << "Endpoint misses: " << missCount << ".  Slept: " << missCount * _sleepTime / 1000 << " seconds"<< std::endl;
            }

        private:
            bool pop(DbOpPointer &dbOp) {
                return _opQueue.pop(dbOp);
            }

            cpp::ThreadPool _threadPool;
            std::string _connection;
            TOpQueue _opQueue;
            size_t _sleepTime;
            size_t _threadCount;
        };


        template <typename TOpQueue = cpp::mtools::OpQueueNoLock>
        class MongoEndPointHolder {
        public:
            using MongoEndPoint = BasicMongoEndPoint<TOpQueue>;
            using MongoEndPointPtr = std::unique_ptr<MongoEndPoint>;
            //Note that ShardName can also be a mongoS, but in the case it doesn't much matter
            using MongoEndPointMap = std::unordered_map<cpp::mtools::MongoCluster::ShardName, MongoEndPointPtr>;

            MongoEndPointHolder(EndPointSettings settings, MongoCluster &mCluster):
                _started(false) {
                if(settings.directLoad) {
                    for(auto &shard: mCluster.shards())
                        _epm.emplace(std::make_pair(shard.first, MongoEndPointPtr(new MongoEndPoint{settings, shard.second})));
                }
                else {
                    for(auto &mongoS: mCluster.mongos())
                        _epm.emplace(std::make_pair(mongoS, MongoEndPointPtr(new MongoEndPoint{settings, mongoS})));
                }
                assert(_epm.size());
                //We like to start with edge cases
                _cycleItr = _epm.begin();
                if(settings.startImmediate)
                    start();
            }

            MongoEndPoint* at(const cpp::mtools::MongoCluster::ShardName &shard) {
                return _epm.at(shard).get();
            }

            MongoEndPoint* getMongoSCycle() {
                cpp::MutexLockGuard lock(_cycleMutex);
                ++_cycleItr;
                if(_cycleItr == _epm.end())
                    _cycleItr = _epm.begin();
                return _cycleItr->second.get();
            }

            bool isRunning() { return _started; }

            void start() {
                _started = true;
                for(auto &i: _epm)
                    i.second->start();
            }

            void gracefulShutdownJoin() {
                for(auto &ep: _epm)
                    ep.second->gracefulShutdownJoin();
            }

        private:
            cpp::Mutex _cycleMutex;
            typename MongoEndPointMap::iterator _cycleItr;
            MongoEndPointMap _epm;
            bool _started;

        };

    } /* namespace mtools */
} /* namespace cpp */

#endif /* MONGOENDPOINT_H_ */
