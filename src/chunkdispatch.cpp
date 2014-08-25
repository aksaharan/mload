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

#include "chunkdispatch.h"

namespace loader {
    namespace dispatch {

        AbstractChunkDispatch::AbstractChunkDispatch(Settings settings) :
                _settings(std::move(settings))
        {
            if (_settings.owner->directLoad()) _ep = _settings.owner->getEndPointForChunk(_settings
                    .chunkUB);
            else _ep = _settings.owner->getMongoSCycle();
        }

        ChunkDispatcher::ChunkDispatcher(Settings settings,
                                               cpp::mtools::MongoCluster &mCluster,
                                               EndPointHolder *eph,
                                               cpp::mtools::MongoCluster::NameSpace ns) :
                _settings(std::move(settings)),
                _tp(_settings.workThreads),
                _mCluster(mCluster),
                _eph(eph),
                _ns(std::move(ns)),
                _loadPlan(_settings.sortIndex)
        {
            _wc.nodes(_settings.writeConcern);
            init();
        }

        void ChunkDispatcher::init() {
            std::unordered_map<cpp::mtools::MongoCluster::ShardName, size_t> shardChunkCounters;
            //Assuming that nsChunks is in sorted order, or the stage setup won't work right
            for (auto &iCm : _mCluster.nsChunks(ns())) {
                _loadPlan.insertUnordered(std::get<0>(iCm), ChunkDispatchPointer {});
                size_t depth = ++(shardChunkCounters[std::get<1>(iCm)->first]);
                //TODO: change this to a factory that creates queues based on chunk depth in the shard
                if (depth <= DIRECT_LOAD)
                    _loadPlan.back() = ImmediateDispatch::create(this, _eph, std::get<0>(iCm));
                else
                    _loadPlan.back() = RAMQueueDispatch::create(this, _eph, std::get<0>(iCm));
            }
        }

        ChunkDispatcher::OrderedWaterFall ChunkDispatcher::getWaterFall() {
            std::unordered_map<cpp::mtools::MongoCluster::ShardName, std::deque<AbstractChunkDispatch*>> chunksort;
            for (auto &i : _mCluster.nsChunks(_ns))
                chunksort[std::get<1>(i)->first].emplace_back(getOpAggForChunk(std::get<0>(i)));
            OrderedWaterFall wf;
            for (;;) {
                bool added = false;
                for (auto &i : chunksort) {
                    auto &q = i.second;
                    if (q.size()) {
                        added = true;
                        wf.push_back(q.back());
                        q.pop_back();
                    }
                }
                if (!added) break;
            }
            return wf;
        }

        void RAMQueueDispatch::doLoad() {
            cpp::mtools::DataQueue sendQueue;
            size_t queueSize = owner()->queueSize();
            for (auto &i : _queue.unSafeAccess()) {
                sendQueue.emplace_back(i.second);
                if (sendQueue.size() >= queueSize) {
                    send(&sendQueue);
                    sendQueue.clear();
                    sendQueue.reserve(queueSize);
                }
            }
        }
    }
}  //namespace loader
