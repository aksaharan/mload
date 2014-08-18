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
#include "inputprocessor.h"
#include "mongocxxdriver.h"


/*
 * Strategy ideas:
 * Testing so far has shown that direct load is great.
 * The disks throughput appears to be completely occupied reading (spiking to 100% util with 2 RAID-0 SSD
 * Cycle sort looks like it might be an option (I think merge sort is going to be too costly in terms of disk use)
 * Also looking for back pressure on the end point queues (atomic size_t?) so we only load the load the lower chunk ranges first
 */
//TODO: Allow for secondary sort key outside of the shard key
//TODO: Support replicas as single member shards

namespace loader {

    Loader::Loader(Settings settings) :
         _settings(std::move(settings)),
         _queueSettings(_settings.queueSettings),
        _mCluster{_settings.connection},
        _endPoints(settings.endPointSettings, _mCluster),
        _opAgg(_settings.opAggSettings, _mCluster, &_endPoints, _settings.ns()),
        _ramMax{cpp::getTotalSystemMemory()},
        _threadsMax{(size_t)_settings.threads},
        _processedFiles{} {
        _writeOps = 0;
        _mCluster.getShardList(&shardList);
        std::cout << _mCluster << std::endl;
    }

    void Loader::setEndPoints() {
        _endPoints.start();
    }


    void Loader::threadProcessFile() {
        InputProcessor lsp(this, _settings.ns());
        for(;;) {
            cpp::LocSegment segment;
            if(!_fileQueue.pop(segment))
                break;
            auto itr = std::find(_locSegMapping.begin(), _locSegMapping.end(), segment);
            assert(itr != _locSegMapping.end());
            lsp.processSegmentToQueue(segment, std::distance(_locSegMapping.begin(), itr));
        }
    }

    opagg::OpAggregator* Loader::getNextPrep() {
        cpp::MutexLockGuard lg(_prepSetMutex);
        if(_wf.empty())
            return nullptr;
        opagg::OpAggregator* ret;
        ret = _wf.front();
        _wf.pop_front();
        return ret;
    }

    void Loader::threadPrepQueue() {
        for(;;) {
            opagg::OpAggregator* prep = getNextPrep();
            if(prep == nullptr)
                break;
            prep->prep();
            prep->doLoad();
        }
    }

    void Loader::run() {
        std::cout << "Threads: " << _settings.threads << " RAM: " << _ramMax / 1024 / 1024  << std::endl;
        std::cout << "Stats: " << _stats << std::endl;
        std::cout << "Starting setup" << std::endl;
        //Load all the files
        using namespace boost::filesystem;
        FileQueue::ContainerType files;
        path loadDir(_settings.loadDir);
        std::regex fileRegex(_settings.fileRegex);
        for(directory_iterator ditr{loadDir}; ditr != directory_iterator{}; ditr++) {
            std::string filename = ditr->path().string();
            if(!is_regular_file(ditr->path()) || (_settings.fileRegex.length() && !std::regex_match(filename,fileRegex)))
                continue;
            files.push_back(filename);
        }

        //Crucial this is sorted!! Using std::find to determine segment numbers
        std::sort(files.begin(), files.end());

        for(auto &i: files)
            _locSegMapping.push_back(i);


        std::cout << "Dir: " << _settings.loadDir << "\nSegments: " << _locSegMapping.size() << std::endl;

        FileQueue::ContainerType fileQ(files);
        _fileQueue.swap(fileQ);

        std::cout //<< this->_mCluster
                << "\nShard Key:" << _settings.shardKeyJson
                << "\nKicking off run"
                << std::endl;


        size_t inputThreads = _threadsMax > files.size() ? files.size() : _threadsMax;
        cpp::ThreadPool tpInput(inputThreads);
        for(size_t i = 0; i < inputThreads; i++)
            tpInput.queue([this]() {this->threadProcessFile(); });
        tpInput.endWaitInitiate();

        if(this->queueSettings().direct) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            setEndPoints();
        }

        size_t finalizeThreads = inputThreads;
        cpp::ThreadPool tpFinalize(finalizeThreads);

        /*
         * Setup for the next part of the run, wait for all input to complete prior to starting next part
         */
        _wf = _opAgg.getWaterFall();
        tpInput.joinAll();
        std::cout << "Entering finalize phase" << std::endl;

        for(size_t i = 0; i < finalizeThreads; i++)
            tpFinalize.queue([this] { this->threadPrepQueue(); });

        if(!enabledEndPoints()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            setEndPoints();
        }

        //Make sure all threads are kicked off
        std::this_thread::sleep_for(std::chrono::seconds(5));

        tpFinalize.endWaitInitiate();
        tpFinalize.joinAll();

        _endPoints.gracefulShutdownJoin();

    }

} /* namespace loader */
