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

#include "loader.h"
#include <algorithm>
#include <exception>
#include <iostream>
#include <regex>
#include <tuple>
#include <parallel/algorithm>
#include <boost/filesystem.hpp>
#include "input_processor.h"
#include "mongo_cxxdriver.h"

/*
 * Strategy ideas:
 * Testing so far has shown that direct load is great when mongoD contains 15M docs or less.
 * The disks throughput appears to be completely occupied reading (spiking to 100% util with 2 RAID-0 SSD
 * Cycle sort looks like it might be an option (I think merge sort is going to be too costly in terms of disk use)
 * Also looking for back pressure on the end point queues (atomic size_t?) so we only load the load the lower chunk ranges first
 */
//TODO: Allow for secondary sort key outside of the shard key
//TODO: Support replicas as single member shards
namespace loader {

    Loader::Loader(Settings settings) :
            _settings(std::move(settings)),
            _mCluster {_settings.connection},
            _endPoints(settings.endPointSettings, _mCluster),
            _chunkDispatch(_settings.dispatchSettings, _mCluster, &_endPoints, _settings.ns()),
            _ramMax {cpp::getTotalSystemMemory()},
            _threadsMax {(size_t) _settings.threads},
            _processedFiles {}
    {
        _writeOps = 0;
        _mCluster.getShardList(&_shardList);
    }

    void Loader::setEndPoints() {
        _endPoints.start();
    }

    void Loader::threadProcessFile() {
        InputProcessor lsp(this, _settings.ns());
        for (;;) {
            cpp::LocSegment segment;
            if (!_fileQueue.pop(segment)) break;
            auto itr = std::find(_locSegMapping.begin(), _locSegMapping.end(), segment);
            assert(itr != _locSegMapping.end());
            lsp.processSegmentToAggregator(segment, std::distance(_locSegMapping.begin(), itr));
        }
    }

    dispatch::AbstractChunkDispatch* Loader::getNextPrep() {
        cpp::MutexLockGuard lg(_prepSetMutex);
        if (_wf.empty()) return nullptr;
        dispatch::AbstractChunkDispatch* ret;
        ret = _wf.front();
        _wf.pop_front();
        return ret;
    }

    void Loader::threadPrepQueue() {
        for (;;) {
            dispatch::AbstractChunkDispatch* prep = getNextPrep();
            if (prep == nullptr) break;
            prep->prep();
            prep->doLoad();
        }
    }

    void Loader::run() {
        /*
         * The hardware parameters we are working with. Note that ram is free RAM when this program
         * started.  i.e. the working ram available.
         */
        std::cout << "Threads: " << _settings.threads << " RAM(Mb): "
                  << _ramMax / 1024 / 1024 << "Starting setup" << std::endl;

        /*
         * Initial setup.  Getting all the files that are going to put into the mognoDs.
         * Every stage is decoupled, there are no guarantees on syncing.
         */
        using namespace boost::filesystem;
        FileQueue::ContainerType files;
        path loadDir(_settings.loadDir);
        std::regex fileRegex(_settings.fileRegex);
        for (directory_iterator ditr {loadDir}; ditr != directory_iterator {}; ditr++) {
            std::string filename = ditr->path().string();
            if (!is_regular_file(ditr->path())
                || (_settings.fileRegex.length() && !std::regex_match(filename, fileRegex))) continue;
            files.push_back(filename);
        }

        /*
         * Crucial this is sorted and not changed past this point.  _locSetMapping is used as an
         * index and it can also have std::sort called against it to find the index of a file name.
         *
         * The various queue stages that need to look up a file index by name or name by index use
         * this and expect to use this as the source of truth for file->index mapping.
         */
        std::sort(files.begin(), files.end());
        for (auto &i : files)
            _locSegMapping.push_back(i);
        FileQueue::ContainerType fileQ(files);
        _fileQueue.swap(fileQ);

        std::cout << "Dir: " << _settings.loadDir << "\nSegments: " << _locSegMapping.size()
                  << "\nShard Key:" << _settings.shardKeyJson << "\nKicking off run" << std::endl;

        /*
         * Start up the threads to read in the files
         */
        size_t inputThreads = _threadsMax > files.size() ? files.size() : _threadsMax;
        cpp::ThreadPool tpInput(inputThreads);
        for (size_t i = 0; i < inputThreads; i++)
            tpInput.queue([this]() {this->threadProcessFile();});
        tpInput.endWaitInitiate();

        /*
         * If direct loading is desired (and it should be 99% of the time) kick that off
         */
        if (this->queueSettings().direct) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            setEndPoints();
        }

        /*
         * After the load is complete hit all queues and call any additional actions.
         * For instance, sort RAM queues.
         * Waterfall means that finalize is called in shard chunk order to minimize possible
         * waiting.
         */
        size_t finalizeThreads = inputThreads;
        cpp::ThreadPool tpFinalize(finalizeThreads);
        _wf = _chunkDispatch.getWaterFall();
        tpInput.joinAll();
        std::cout << "Entering finalize phase" << std::endl;

        for (size_t i = 0; i < finalizeThreads; i++)
            tpFinalize.queue([this] {this->threadPrepQueue();});

        if (!enabledEndPoints()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            setEndPoints();
        }

        //Make sure all threads are kicked off
        std::this_thread::sleep_for(std::chrono::seconds(5));

        /*
         *  Wait for all threads to shutdown prior to exit.
         */
        tpFinalize.endWaitInitiate();
        tpFinalize.joinAll();

        _endPoints.gracefulShutdownJoin();

    }

}  //namespace loader
