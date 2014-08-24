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
#include "inputqueue.h"
#include "mongoendpoint.h"
#include "chunkdispatch.h"
#include "tools.h"
#include "threading.h"

namespace loader {

    /**
     *  Loader does all the work actually loading the mongoDs.
     *  The main function in this class is run() which kicks the load off.
     *  _mCluster must be accessed read only after initialization.
     *  Loader assumes the balancer is turned off (we don't want the wasted efficient of chunk
     *  moves while loading so this is reasonable and saves some work.
     */

    class Loader {
    public:
        using MissTime = std::chrono::milliseconds;
        /**
         * LoaderStats is currently "dead".  It is being kept around for the next round of optimizations.
         */
        struct LoaderStats {
            std::atomic<size_t> feederMisses;
            MissTime feederMissTime;
            size_t docFails;

            LoaderStats() :
                    feederMisses(), feederMissTime(), docFails()
            {
            }

        };

        explicit Loader(Settings settings);

        /**
         * Gets stats
         */
        const LoaderStats& stats() const {
            return _stats;
        }

        /**
         * Get cluster
         * Must be read only in multithreaded mode
         */
        cpp::mtools::MongoCluster& cluster() {
            return _mCluster;
        }

        /**
         * Returns the opAggregator queues
         */
        dispatch::ChunkDispatcher& opAgg() {
            return _opDispatch;
        }

        /**
         * Returns the settings.
         */
        const Settings& settings() const {
            return _settings;
        }

        /**
         * run() kicks off the loading process.
         */
        void run();

        /**
         * Returns the settings for loader queues.
         */
        const queue::Settings& queueSettings() const {
            return _queueSettings;
        }

        ~Loader() {
        }

    private:
        using FileQueue = cpp::ConcurrentQueue<cpp::LocSegment>;
        using IndexObj = mongo::BSONObj;

        LoaderStats _stats;
        const Settings _settings;
        queue::Settings _queueSettings;
        cpp::mtools::MongoCluster _mCluster;
        EndPointHolder _endPoints;
        dispatch::ChunkDispatcher _opDispatch;

        std::deque<std::string> shardList;
        size_t _ramMax;
        size_t _threadsMax;
        std::atomic<std::size_t> _processedFiles;
        std::atomic<unsigned long long> _writeOps;
        FileQueue _fileQueue;
        cpp::LocSegMapping _locSegMapping;
        dispatch::ChunkDispatcher::OrderedWaterFall _wf;
        cpp::Mutex _prepSetMutex;

        bool enabledEndPoints() {
            return _endPoints.isRunning();
        }

        /**
         * Start end points up
         */
        void setEndPoints();

        /**
         * Creates the objects to read in the files and executes them.
         * Thread safe
         */
        void threadProcessFile();

        /**
         * Creates objects and runs the notifications to the queues that the load process has
         * completed reading in all the files.
         * Thread safe
         */
        void threadPrepQueue();

        /**
         * Get the next chunk to notify of input file completion in shard chunk order.
         * Thread safe
         */
        dispatch::AbstractChunkDispatch* getNextPrep();

        /**
         * Resolves a connection for a shard
         */
        const std::string& getConn(const std::string &shard) {
            return this->_mCluster.getConn(shard);
        }
    };
} //namespace loader

#endif /* LOADER_H_ */
