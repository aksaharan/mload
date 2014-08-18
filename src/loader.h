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

#ifndef LOADER_H_
#define LOADER_H_


#include <vector>
#include <fstream>
#include <mongo/client/dbclient.h>
#include "bsontools.h"
#include "concurrentcontainer.h"
#include "inputprocessor.h"
#include "loaderdefs.h"
#include "loadqueue.h"
#include "mongoendpoint.h"
#include "opaggregator.h"
#include "tools.h"
#include "threading.h"

//#define LOADER_SINGLE_THREADING

namespace loader {


    class Loader {
    public:
        using MissTime = std::chrono::milliseconds;
        struct LoaderStats {
            std::atomic<size_t> feederMisses;
            MissTime feederMissTime;
            size_t docFails;

            LoaderStats(): feederMisses(), feederMissTime(), docFails() { }

            friend std::ostream& operator<< (std::ostream &out, const LoaderStats &ls) {
                //TODO: get misses working again in a reasonable manner
                //out << "Feeder Misses: " << ls.feederMisses;// << " Loader Wait Time(ms):" << ns.count();
                return out;
            }
        };

        Loader(Settings settings);

        const LoaderStats& stats() { return _stats; }

        cpp::mtools::MongoCluster& cluster() { return  _mCluster; }
        opagg::OpAggregatorHolder& opAgg() { return _opAgg; }
        const Settings& settings() const { return _settings; }

        void run();

        bool loadComplete(const cpp::mtools::MongoCluster::ShardName &shard) {
            assert(false);
            return false;
        }

        const queue::Settings& queueSettings() const { return _queueSettings; }

        ~Loader() {}

    private:
        using FileQueue = cpp::ConcurrentQueue<cpp::LocSegment>;
        using IndexObj = mongo::BSONObj;

        LoaderStats _stats;
        const Settings _settings;
        queue::Settings _queueSettings;
        cpp::mtools::MongoCluster _mCluster;
        EndPointHolder _endPoints;
        opagg::OpAggregatorHolder _opAgg;

        std::deque<std::string> shardList;
        size_t _ramMax;
        size_t _threadsMax;
        std::atomic<std::size_t> _processedFiles;
        std::atomic<unsigned long long> _writeOps;
        FileQueue _fileQueue;
        cpp::LocSegMapping _locSegMapping;
        opagg::OpAggregatorHolder::OrderedWaterFall _wf;
        cpp::Mutex _prepSetMutex;

        bool enabledEndPoints() { return _endPoints.isRunning(); }

        void setEndPoints();

        void threadProcessFile();
        void threadPrepQueue();
        opagg::OpAggregator* getNextPrep();

        class PrepQueue {
        public:
            Loader &_owner;
            const std::string _shard;

            PrepQueue(Loader &owner, std::string shard) : _owner(owner), _shard(shard) { }

            void run();
        };

        const std::string& getConn(const std::string &shard) {
            return this->_mCluster.getConn(shard);
        }
    };
} //namespace loader

#endif /* LOADER_H_ */
